// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, mem, sync::Arc};

use arrow::{
    array::{make_array, Array, ArrayData, ArrayRef},
    buffer::MutableBuffer,
    compute,
    record_batch::RecordBatch as ArrowRecordBatch,
    util::bit_util,
};
use async_trait::async_trait;
use bytes_ext::{BytesMut, SafeBufMut};
use ceresdbproto::sst as sst_pb;
use common_types::{
    datum::DatumKind,
    schema::{ArrowSchema, ArrowSchemaRef, DataType, Field, Schema},
};
use generic_error::{BoxError, GenericError};
use log::trace;
use macros::define_result;
use parquet::{
    arrow::AsyncArrowWriter,
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use prost::Message;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::io::AsyncWrite;

use crate::sst::parquet::{
    hybrid::{self, IndexedType},
    meta_data::ParquetMetaData,
};

// TODO: Only support i32 offset now, consider i64 here?
const OFFSET_SIZE: usize = std::mem::size_of::<i32>();

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
    "Failed to encode sst meta data, err:{}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    EncodeIntoPb {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to decode sst meta data, base64 of meta value:{}, err:{}.\nBacktrace:\n{}",
    meta_value,
    source,
    backtrace,
    ))]
    DecodeFromPb {
        meta_value: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Invalid meta key, expect:{}, given:{}.\nBacktrace:\n{}",
    expect,
    given,
    backtrace
    ))]
    InvalidMetaKey {
        expect: String,
        given: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Base64 meta value not found.\nBacktrace:\n{}", backtrace))]
    Base64MetaValueNotFound { backtrace: Backtrace },

    #[snafu(display(
    "Invalid base64 meta value length, base64 of meta value:{}.\nBacktrace:\n{}",
    meta_value,
    backtrace,
    ))]
    InvalidBase64MetaValueLen {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to decode base64 meta value, base64 of meta value:{}, err:{}",
    meta_value,
    source
    ))]
    DecodeBase64MetaValue {
        meta_value: String,
        source: base64::DecodeError,
    },

    #[snafu(display(
    "Invalid meta value length, base64 of meta value:{}.\nBacktrace:\n{}",
    meta_value,
    backtrace
    ))]
    InvalidMetaValueLen {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Invalid meta value header, base64 of meta value:{}.\nBacktrace:\n{}",
    meta_value,
    backtrace
    ))]
    InvalidMetaValueHeader {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert sst meta data from protobuf, err:{}", source))]
    ConvertSstMetaData {
        source: crate::sst::parquet::meta_data::Error,
    },

    #[snafu(display(
    "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    EncodeRecordBatch {
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to decode hybrid record batch, err:{}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    DecodeRecordBatch {
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Sst meta data collapsible_cols_idx is empty, fail to decode hybrid record batch.\nBacktrace:\n{}",
    backtrace
    ))]
    CollapsibleColsIdxEmpty { backtrace: Backtrace },

    #[snafu(display("Tsid is required for hybrid format.\nBacktrace:\n{}", backtrace))]
    TsidRequired { backtrace: Backtrace },

    #[snafu(display(
    "Key column must be string type. type:{}\nBacktrace:\n{}",
    type_name,
    backtrace
    ))]
    StringKeyColumnRequired {
        type_name: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub const META_KEY: &str = "meta";
pub const META_VALUE_HEADER: u8 = 0;

/// Encode the sst meta data into binary key value pair.
pub fn encode_sst_meta_data(meta_data: ParquetMetaData) -> Result<KeyValue> {
    let meta_data_pb = sst_pb::ParquetMetaData::from(meta_data);

    let mut buf = BytesMut::with_capacity(meta_data_pb.encoded_len() + 1);
    buf.try_put_u8(META_VALUE_HEADER)
        .expect("Should write header into the buffer successfully");

    // encode the sst meta data into protobuf binary
    meta_data_pb.encode(&mut buf).context(EncodeIntoPb)?;
    Ok(KeyValue {
        key: META_KEY.to_string(),
        value: Some(base64::encode(buf.as_ref())),
    })
}

/// Decode the sst meta data from the binary key value pair.
pub fn decode_sst_meta_data(kv: &KeyValue) -> Result<ParquetMetaData> {
    ensure!(
        kv.key == META_KEY,
        InvalidMetaKey {
            expect: META_KEY,
            given: &kv.key,
        }
    );

    let meta_value = kv.value.as_ref().context(Base64MetaValueNotFound)?;
    ensure!(
        !meta_value.is_empty(),
        InvalidBase64MetaValueLen { meta_value }
    );

    let raw_bytes = base64::decode(meta_value).context(DecodeBase64MetaValue { meta_value })?;

    ensure!(!raw_bytes.is_empty(), InvalidMetaValueLen { meta_value });

    ensure!(
        raw_bytes[0] == META_VALUE_HEADER,
        InvalidMetaValueHeader { meta_value }
    );

    let meta_data_pb: sst_pb::ParquetMetaData =
        Message::decode(&raw_bytes[1..]).context(DecodeFromPb { meta_value })?;

    ParquetMetaData::try_from(meta_data_pb).context(ConvertSstMetaData)
}

/// RecordEncoder is used for encoding ArrowBatch.
///
/// TODO: allow pre-allocate buffer
#[async_trait]
trait RecordEncoder {
    /// Encode vector of arrow batch, return encoded row number
    async fn encode(&mut self, arrowRecordBatchVec: Vec<ArrowRecordBatch>) -> Result<usize>;

    fn set_meta_data(&mut self, meta_data: ParquetMetaData) -> Result<()>;

    /// Return encoded bytes
    /// Note: trait method cannot receive `self`, so take a &mut self here to
    /// indicate this encoder is already consumed
    async fn close(&mut self) -> Result<()>;
}

struct ColumnarRecordEncoder<W> {
    // wrap in Option so ownership can be taken out behind `&mut self`
    asyncArrowWriter: Option<AsyncArrowWriter<W>>,
    arrowSchema: ArrowSchemaRef,
}

impl<W: AsyncWrite + Send + Unpin> ColumnarRecordEncoder<W> {
    fn new(writer: W,
           schema: &Schema,
           num_rows_per_row_group: usize,
           max_buffer_size: usize,
           compression: Compression) -> Result<Self> {
        let arrow_schema = schema.to_arrow_schema_ref();

        let write_props = WriterProperties::builder()
            .set_max_row_group_size(num_rows_per_row_group)
            .set_compression(compression)
            .build();

        let arrow_writer =
            AsyncArrowWriter::try_new(writer,
                                      arrow_schema.clone(),
                                      max_buffer_size,
                                      Some(write_props)).box_err().context(EncodeRecordBatch)?;

        Ok(Self {
            asyncArrowWriter: Some(arrow_writer),
            arrowSchema: arrow_schema,
        })
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> RecordEncoder for ColumnarRecordEncoder<W> {
    async fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize> {
        assert!(self.asyncArrowWriter.is_some());

        let record_batch = compute::concat_batches(&self.arrowSchema, &arrow_record_batch_vec).box_err().context(EncodeRecordBatch)?;

        self.asyncArrowWriter.as_mut().unwrap().write(&record_batch).await.box_err().context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    fn set_meta_data(&mut self, meta_data: ParquetMetaData) -> Result<()> {
        let key_value = encode_sst_meta_data(meta_data)?;
        self.asyncArrowWriter
            .as_mut()
            .unwrap()
            .append_key_value_metadata(key_value);

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        assert!(self.asyncArrowWriter.is_some());

        let arrow_writer = self.asyncArrowWriter.take().unwrap();
        arrow_writer
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(())
    }
}

struct HybridRecordEncoder<W> {
    // wrap in Option so ownership can be taken out behind `&mut self`
    arrow_writer: Option<AsyncArrowWriter<W>>,
    arrow_schema: ArrowSchemaRef,
    tsid_type: IndexedType,
    non_collapsible_col_types: Vec<IndexedType>,
    // columns that can be collapsed into list
    collapsible_col_types: Vec<IndexedType>,
    collapsible_col_idx: Vec<u32>,
}

impl<W: AsyncWrite + Unpin + Send> HybridRecordEncoder<W> {
    fn new(writer: W,
           schema: &Schema,
           num_rows_per_row_group: usize,
           max_buffer_size: usize,
           compression: Compression) -> Result<Self> {
        // TODO: What we really want here is a unique ID, tsid is one case
        // Maybe support other cases later.
        let tsid_idx = schema.index_of_tsid().context(TsidRequired)?;
        let tsid_type = IndexedType {
            idx: tsid_idx,
            data_type: schema.column(tsid_idx).datumKind,
        };

        let mut non_collapsible_col_types = Vec::new();
        let mut collapsible_col_types = Vec::new();
        let mut collapsible_col_idx = Vec::new();
        for (idx, col) in schema.columns().iter().enumerate() {
            if idx == tsid_idx {
                continue;
            }

            if schema.is_collapsible_column(idx) {
                collapsible_col_types.push(IndexedType {
                    idx,
                    data_type: schema.column(idx).datumKind,
                });
                collapsible_col_idx.push(idx as u32);
            } else {
                // TODO: support non-string key columns
                ensure!(
                    matches!(col.datumKind, DatumKind::String),
                    StringKeyColumnRequired {
                        type_name: col.datumKind.to_string(),
                    }
                );
                non_collapsible_col_types.push(IndexedType {
                    idx,
                    data_type: col.datumKind,
                });
            }
        }

        let arrow_schema = hybrid::build_hybrid_arrow_schema(schema);

        let write_props = WriterProperties::builder()
            .set_max_row_group_size(num_rows_per_row_group)
            .set_compression(compression)
            .build();

        let arrow_writer = AsyncArrowWriter::try_new(
            writer,
            arrow_schema.clone(),
            max_buffer_size,
            Some(write_props),
        )
            .box_err()
            .context(EncodeRecordBatch)?;
        Ok(Self {
            arrow_writer: Some(arrow_writer),
            arrow_schema,
            tsid_type,
            non_collapsible_col_types,
            collapsible_col_types,
            collapsible_col_idx,
        })
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> RecordEncoder for HybridRecordEncoder<W> {
    async fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize> {
        assert!(self.arrow_writer.is_some());

        let record_batch = hybrid::convert_to_hybrid_record(
            &self.tsid_type,
            &self.non_collapsible_col_types,
            &self.collapsible_col_types,
            self.arrow_schema.clone(),
            arrow_record_batch_vec,
        )
            .box_err()
            .context(EncodeRecordBatch)?;

        self.arrow_writer
            .as_mut()
            .unwrap()
            .write(&record_batch)
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    fn set_meta_data(&mut self, mut meta_data: ParquetMetaData) -> Result<()> {
        meta_data.collapsible_cols_idx = mem::take(&mut self.collapsible_col_idx);
        let key_value = encode_sst_meta_data(meta_data)?;
        self.arrow_writer
            .as_mut()
            .unwrap()
            .append_key_value_metadata(key_value);

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        assert!(self.arrow_writer.is_some());

        let arrow_writer = self.arrow_writer.take().unwrap();
        arrow_writer
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(())
    }
}

pub struct ParquetEncoder {
    recordEncoder: Box<dyn RecordEncoder + Send>,
}

impl ParquetEncoder {
    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(writer: W,
                                                       schema: &Schema,
                                                       useHybridEncoding: bool,
                                                       num_rows_per_row_group: usize,
                                                       max_buffer_size: usize,
                                                       compression: Compression) -> Result<Self> {
        let recordEncoder: Box<dyn RecordEncoder + Send> = if useHybridEncoding {
            Box::new(HybridRecordEncoder::new(writer,
                                              schema,
                                              num_rows_per_row_group,
                                              max_buffer_size,
                                              compression)?)
        } else {
            Box::new(ColumnarRecordEncoder::new(writer,
                                                schema,
                                                num_rows_per_row_group,
                                                max_buffer_size,
                                                compression)?)
        };

        Ok(ParquetEncoder { recordEncoder })
    }

    /// encode the record batch with [ArrowWriter] and the encoded contents is written to the buffer.
    pub async fn encode(&mut self, arrowRecordBatchVec: Vec<ArrowRecordBatch>) -> Result<usize> {
        if arrowRecordBatchVec.is_empty() {
            return Ok(0);
        }

        self.recordEncoder.encode(arrowRecordBatchVec).await
    }

    pub fn set_meta_data(&mut self, meta_data: ParquetMetaData) -> Result<()> {
        self.recordEncoder.set_meta_data(meta_data)
    }

    pub async fn close(mut self) -> Result<()> {
        self.recordEncoder.close().await
    }
}

/// RecordDecoder is used for decoding ArrowRecordBatch based on
/// `schema.StorageFormat`
trait RecordDecoder {
    fn decode(&self, arrow_record_batch: ArrowRecordBatch) -> Result<ArrowRecordBatch>;
}

struct ColumnarRecordDecoder {}

impl RecordDecoder for ColumnarRecordDecoder {
    fn decode(&self, arrow_record_batch: ArrowRecordBatch) -> Result<ArrowRecordBatch> {
        Ok(arrow_record_batch)
    }
}

struct HybridRecordDecoder {
    collapsible_cols_idx: Vec<u32>,
}

impl HybridRecordDecoder {
    /// Convert `ListArray` fields to underlying data type
    fn convert_schema(arrow_schema: ArrowSchemaRef) -> ArrowSchemaRef {
        let new_fields: Vec<_> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                if let DataType::List(nested_field) = f.data_type() {
                    match f.data_type() {
                        DataType::Dictionary(_, _) => {
                            assert!(f.dict_id().is_some(), "Dictionary must have dict_id");
                            assert!(
                                f.dict_is_ordered().is_some(),
                                "Dictionary must have dict_is_ordered"
                            );
                            let dict_id = f.dict_id().unwrap();
                            let dict_is_ordered = f.dict_is_ordered().unwrap();
                            Arc::new(Field::new_dict(
                                f.name(),
                                nested_field.data_type().clone(),
                                true,
                                dict_id,
                                dict_is_ordered,
                            ))
                        }
                        _ => Arc::new(Field::new(f.name(), nested_field.data_type().clone(), true)),
                    }
                } else {
                    f.clone()
                }
            })
            .collect();
        Arc::new(ArrowSchema::new_with_metadata(
            new_fields,
            arrow_schema.metadata().clone(),
        ))
    }

    /// Stretch hybrid collapsed column into columnar column.
    /// `value_offsets` specify offsets each value occupied, which means that
    /// the number of a `value[n]` is `value_offsets[n] - value_offsets[n-1]`.
    /// Ex:
    ///
    /// `array_ref` is `a b c`, `value_offsets` is `[0, 3, 5, 6]`, then
    /// output array is `a a a b b c`
    ///
    /// Note: caller should ensure offsets is not empty.
    fn stretch_variable_length_column(
        array_ref: &ArrayRef,
        value_offsets: &[i32],
    ) -> Result<ArrayRef> {
        assert_eq!(array_ref.len() + 1, value_offsets.len());

        let values_num = *value_offsets.last().unwrap() as usize;
        let array_data = array_ref.to_data();
        let offset_slices = array_data.buffers()[0].as_slice();
        let value_slices = array_data.buffers()[1].as_slice();
        let nulls = array_data.nulls();
        trace!(
            "raw buffer slice, offsets:{:#02x?}, values:{:#02x?}",
            offset_slices,
            value_slices,
        );

        let i32_offsets = Self::get_array_offsets(offset_slices);
        let mut value_bytes = 0;
        for (idx, (current, prev)) in i32_offsets[1..].iter().zip(&i32_offsets).enumerate() {
            let value_len = current - prev;
            let value_num = value_offsets[idx + 1] - value_offsets[idx];
            value_bytes += value_len * value_num;
        }

        // construct new expanded array
        let mut new_offsets_buffer = MutableBuffer::new(OFFSET_SIZE * values_num);
        let mut new_values_buffer = MutableBuffer::new(value_bytes as usize);
        let mut new_null_buffer = hybrid::new_ones_buffer(values_num);
        let null_slice = new_null_buffer.as_slice_mut();
        let mut value_length_so_far: i32 = 0;
        new_offsets_buffer.push(value_length_so_far);
        let mut bitmap_length_so_far: usize = 0;

        for (idx, (current, prev)) in i32_offsets[1..].iter().zip(&i32_offsets).enumerate() {
            let value_len = current - prev;
            let value_num = value_offsets[idx + 1] - value_offsets[idx];

            if let Some(nulls) = nulls {
                if nulls.is_null(idx) {
                    for i in 0..value_num {
                        bit_util::unset_bit(null_slice, bitmap_length_so_far + i as usize);
                    }
                }
            }
            bitmap_length_so_far += value_num as usize;
            new_values_buffer
                .extend(value_slices[*prev as usize..*current as usize].repeat(value_num as usize));
            for _ in 0..value_num {
                value_length_so_far += value_len;
                new_offsets_buffer.push(value_length_so_far);
            }
        }
        trace!(
            "new buffer slice, offsets:{:#02x?}, values:{:#02x?}, bitmap:{:#02x?}",
            new_offsets_buffer.as_slice(),
            new_values_buffer.as_slice(),
            new_null_buffer.as_slice(),
        );

        let array_data = ArrayData::builder(array_ref.data_type().clone())
            .len(values_num)
            .add_buffer(new_offsets_buffer.into())
            .add_buffer(new_values_buffer.into())
            .null_bit_buffer(Some(new_null_buffer.into()))
            .build()
            .box_err()
            .context(DecodeRecordBatch)?;

        Ok(make_array(array_data))
    }

    /// Like `stretch_variable_length_column`, but array value is fixed-size
    /// type.
    ///
    /// Note: caller should ensure offsets is not empty.
    fn stretch_fixed_length_column(
        array_ref: &ArrayRef,
        value_size: usize,
        value_offsets: &[i32],
    ) -> Result<ArrayRef> {
        assert!(!value_offsets.is_empty());

        let values_num = *value_offsets.last().unwrap() as usize;
        let array_data = array_ref.to_data();
        let old_values_buffer = array_data.buffers()[0].as_slice();
        let old_nulls = array_data.nulls();

        let mut new_values_buffer = MutableBuffer::new(value_size * values_num);
        let mut new_null_buffer = hybrid::new_ones_buffer(values_num);
        let null_slice = new_null_buffer.as_slice_mut();
        let mut length_so_far = 0;

        for (idx, offset) in (0..old_values_buffer.len()).step_by(value_size).enumerate() {
            let value_num = (value_offsets[idx + 1] - value_offsets[idx]) as usize;
            if let Some(nulls) = old_nulls {
                if nulls.is_null(idx) {
                    for i in 0..value_num {
                        bit_util::unset_bit(null_slice, length_so_far + i);
                    }
                }
            }
            length_so_far += value_num;
            new_values_buffer
                .extend(old_values_buffer[offset..offset + value_size].repeat(value_num))
        }
        let array_data = ArrayData::builder(array_ref.data_type().clone())
            .add_buffer(new_values_buffer.into())
            .null_bit_buffer(Some(new_null_buffer.into()))
            .len(values_num)
            .build()
            .box_err()
            .context(DecodeRecordBatch)?;

        Ok(make_array(array_data))
    }

    /// Decode offset slices into Vec<i32>
    fn get_array_offsets(offset_slices: &[u8]) -> Vec<i32> {
        let mut i32_offsets = Vec::with_capacity(offset_slices.len() / OFFSET_SIZE);
        for i in (0..offset_slices.len()).step_by(OFFSET_SIZE) {
            let offset = i32::from_le_bytes(offset_slices[i..i + OFFSET_SIZE].try_into().unwrap());
            i32_offsets.push(offset);
        }

        i32_offsets
    }
}

impl RecordDecoder for HybridRecordDecoder {
    /// Decode records from hybrid to columnar format
    fn decode(&self, arrow_record_batch: ArrowRecordBatch) -> Result<ArrowRecordBatch> {
        let new_arrow_schema = Self::convert_schema(arrow_record_batch.schema());
        let arrays = arrow_record_batch.columns();

        let mut value_offsets = None;
        // Find value offsets from the first col in collapsible_cols_idx.
        if let Some(idx) = self.collapsible_cols_idx.first() {
            let array_data = arrays[*idx as usize].to_data();
            let offset_slices = array_data.buffers()[0].as_slice();
            value_offsets = Some(Self::get_array_offsets(offset_slices));
        } else {
            CollapsibleColsIdxEmpty.fail()?;
        }

        let value_offsets = value_offsets.unwrap();
        let arrays = arrays
            .iter()
            .map(|array_ref| {
                let data_type = array_ref.data_type();
                match data_type {
                    // TODO:
                    // 1. we assume the datatype inside the List is primitive now
                    // Ensure this when create table
                    // 2. Although nested structure isn't support now, but may will someday in
                    // future. So We should keep metadata about which columns
                    // are collapsed by hybrid storage format, to differentiate
                    // List column in original records
                    DataType::List(_nested_field) => {
                        Ok(make_array(array_ref.to_data().child_data()[0].clone()))
                    }
                    _ => {
                        let datum_kind = DatumKind::from_data_type(data_type).unwrap();
                        match datum_kind.size() {
                            None => Self::stretch_variable_length_column(array_ref, &value_offsets),
                            Some(value_size) => Self::stretch_fixed_length_column(
                                array_ref,
                                value_size,
                                &value_offsets,
                            ),
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        ArrowRecordBatch::try_new(new_arrow_schema, arrays)
            .box_err()
            .context(EncodeRecordBatch)
    }
}

pub struct ParquetDecoder {
    record_decoder: Box<dyn RecordDecoder>,
}

impl ParquetDecoder {
    pub fn new(collapsible_cols_idx: &[u32]) -> Self {
        let record_decoder: Box<dyn RecordDecoder> = if collapsible_cols_idx.is_empty() {
            Box::new(ColumnarRecordDecoder {})
        } else {
            Box::new(HybridRecordDecoder {
                collapsible_cols_idx: collapsible_cols_idx.to_vec(),
            })
        };

        Self { record_decoder }
    }

    pub fn decode_record_batch(
        &self,
        arrow_record_batch: ArrowRecordBatch,
    ) -> Result<ArrowRecordBatch> {
        self.record_decoder.decode(arrow_record_batch)
    }
}
// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utilities for `RecordBatch` serialization using Arrow IPC

use std::{borrow::Cow, io::Cursor, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ArrowError {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Zstd decode error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ZstdError {
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum CompressionMethod {
    #[default]
    None,
    Zstd,
}

// https://facebook.github.io/zstd/zstd_manual.html
// The lower the level, the faster the speed (at the cost of compression).
const ZSTD_LEVEL: i32 = 3;

#[derive(Default)]
/// Encoder that can encode a batch of record batches with specific compression
/// options.
pub struct RecordBatchesEncoder {
    stream_writer: Option<StreamWriter<Vec<u8>>>,
    num_rows: usize,
    /// Whether the writer has more than one dict fields, we need to do schema
    /// convert.
    cached_converted_schema: Option<SchemaRef>,
    compress_opts: CompressOptions,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct CompressOptions {
    /// The minimum length of the payload to be compressed.
    pub compress_min_length: usize,
    pub method: CompressionMethod,
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
            method: CompressionMethod::Zstd,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct CompressOutput {
    pub method: CompressionMethod,
    pub payload: Vec<u8>,
}

impl CompressOutput {
    #[inline]
    pub fn no_compression(payload: Vec<u8>) -> Self {
        Self {
            method: CompressionMethod::None,
            payload,
        }
    }
}

impl CompressOptions {
    pub fn maybe_compress(&self, input: Vec<u8>) -> Result<CompressOutput> {
        if input.len() < self.compress_min_length {
            return Ok(CompressOutput::no_compression(input));
        }

        match self.method {
            CompressionMethod::None => Ok(CompressOutput::no_compression(input)),
            CompressionMethod::Zstd => {
                let payload = zstd::bulk::compress(&input, ZSTD_LEVEL).context(ZstdError)?;
                Ok(CompressOutput {
                    method: CompressionMethod::Zstd,
                    payload,
                })
            }
        }
    }
}

impl RecordBatchesEncoder {
    pub fn new(compress_opts: CompressOptions) -> Self {
        Self {
            stream_writer: None,
            num_rows: 0,
            cached_converted_schema: None,
            compress_opts,
        }
    }

    /// Get the number of rows that have been encoded.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// When schema more than one dict fields, it will return a new owned
    /// schema, otherwise it just return the origin schema.
    ///
    /// Workaround for https://github.com/apache/arrow-datafusion/issues/6784
    fn convert_schema(schema: &SchemaRef) -> Cow<SchemaRef> {
        let dict_field_num: usize = schema
            .fields()
            .iter()
            .map(|f| {
                if let DataType::Dictionary(_, _) = f.data_type() {
                    1
                } else {
                    0
                }
            })
            .sum();
        if dict_field_num <= 1 {
            return Cow::Borrowed(schema);
        }

        let new_fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if let DataType::Dictionary(_, _) = f.data_type() {
                    let dict_id = i as i64;
                    Arc::new(Field::new_dict(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                        dict_id,
                        f.dict_is_ordered().unwrap_or(false),
                    ))
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>();

        let schema_ref = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata.clone(),
        ));

        Cow::Owned(schema_ref)
    }

    /// Append one batch into the encoder for encoding.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let stream_writer = if let Some(v) = &mut self.stream_writer {
            v
        } else {
            let mem_size = batch
                .columns()
                .iter()
                .map(|col| col.get_buffer_memory_size())
                .sum();
            let buffer: Vec<u8> = Vec::with_capacity(mem_size);
            let schema = batch.schema();
            let schema = Self::convert_schema(&schema);
            let stream_writer = StreamWriter::try_new(buffer, &schema).context(ArrowError)?;
            if schema.is_owned() {
                self.cached_converted_schema = Some(schema.into_owned());
            }
            self.stream_writer = Some(stream_writer);
            self.stream_writer.as_mut().unwrap()
        };

        if let Some(schema) = &self.cached_converted_schema {
            let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                .context(ArrowError)?;
            stream_writer.write(&batch).context(ArrowError)?;
        } else {
            stream_writer.write(batch).context(ArrowError)?;
        }
        self.num_rows += batch.num_rows();
        Ok(())
    }

    /// Finish encoding and generate the final encoded bytes, which may be
    /// compressed.
    pub fn finish(mut self) -> Result<CompressOutput> {
        let stream_writer = match self.stream_writer.take() {
            None => return Ok(CompressOutput::no_compression(Vec::new())),
            Some(v) => v,
        };

        let encoded_bytes = stream_writer.into_inner().context(ArrowError)?;
        self.compress_opts.maybe_compress(encoded_bytes)
    }
}

/// Encode one record batch with given compression.
pub fn encode_record_batch(
    batch: &RecordBatch,
    compress_opts: CompressOptions,
) -> Result<CompressOutput> {
    let mut encoder = RecordBatchesEncoder::new(compress_opts);
    encoder.write(batch)?;
    encoder.finish()
}

/// Decode multiple record batches from the encoded bytes.
pub fn decode_record_batches(
    bytes: Vec<u8>,
    compression: CompressionMethod,
) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = match compression {
        CompressionMethod::None => bytes,
        CompressionMethod::Zstd => {
            zstd::stream::decode_all(Cursor::new(bytes)).context(ZstdError)?
        }
    };

    let stream_reader = StreamReader::try_new(Cursor::new(bytes), None).context(ArrowError)?;
    stream_reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context(ArrowError)
}
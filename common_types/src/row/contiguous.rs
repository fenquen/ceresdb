// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contiguous row.

use std::{
    convert::TryInto,
    debug_assert_eq, fmt, mem,
    ops::{Deref, DerefMut},
    str,
};

use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    datum::{Datum, DatumKind, DatumView},
    projected_schema::RowProjector,
    row::{
        bitset::{BitSet, RoBitSet},
        Row,
    },
    schema::{IndexInWriterSchema, Schema},
    time::Timestamp,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("string is too long to encode into row (max is {MAX_STRING_LEN}), len:{len}.\nBacktrace:\n{backtrace}", ))]
    StringTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display("row is too long to encode(max is {MAX_ROW_LEN}), len:{len}.\nBacktrace:\n{backtrace}"))]
    RowTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display("number of null columns is missing.\nBacktrace:\n{backtrace}"))]
    NumNullColsMissing { backtrace: Backtrace },

    #[snafu(display("the raw bytes of bit set is invalid, expect_len:{expect_len}, give_len:{given_len}.\nBacktrace:\n{backtrace}"))]
    InvalidBitSetBytes {
        expect_len: usize,
        given_len: usize,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Offset used in row's encoding
type Offset = u32;

/// Max allowed string length of datum to store in a contiguous row (16 MB).
const MAX_STRING_LEN: usize = 1024 * 1024 * 16;
/// Max allowed length of total bytes in a contiguous row (1 GB).
const MAX_ROW_LEN: usize = 1024 * 1024 * 1024;

/// Row encoded in a contiguous buffer.
pub trait ContiguousRow {
    /// return the number of datums.
    fn num_datum_views(&self) -> usize;

    /// return [DatumView] of column in given index.
    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView;
}

/// here is the layout of the encoded continuous row:
/// ```plaintext
/// +------------------+-----------------+-------------------------+-------------------------+
/// | num_bits(u32)    |  nulls_bit_set  | datum encoding block... | var-len payload block   |
/// +------------------+-----------------+-------------------------+-------------------------+
/// ```
/// The first block is the number of bits of the `nulls_bit_set`, which is used
/// to rebuild the bit set. The `nulls_bit_set` is used to record which columns
/// are null. With the bitset, any null column won't be encoded in the following
/// datum encoding block.
///
/// And if `num_bits` is equal to zero, it will still take 4B while the
/// `nulls_bit_set` block will be ignored.
///
/// As for the datum encoding block, most type shares the similar pattern:
/// ```plaintext
/// +----------------+
/// | payload/offset |
/// +----------------+
/// ```
/// If the type has a fixed size, here will be the data payload.
/// Otherwise, a offset in the var-len payload block pointing the real payload.
struct Encoding;

impl Encoding {
    const fn offsetSize() -> usize {
        mem::size_of::<u32>()
    }

    /// 名字有待改善 由目前的使用场合来看应该是rowData的打头的用来记录总的column数量的区域的大小
    const fn byteNumSize() -> usize {
        mem::size_of::<u32>()
    }
}

pub enum ContiguousRowReader<'a, T> {
    NoNulls(ContiguousRowReaderNoNulls<'a, T>),
    WithNulls(ContiguousRowReaderWithNulls<'a, T>),
}

impl<'a, T: Deref<Target=[u8]>> ContiguousRowReader<'a, T> {
    pub fn new(rowData: &'a T, schema: &'a Schema) -> Result<Self> {
        //let byte_offsets = schema.columnByteOffsetVec();

        ensure!(rowData.len() >= Encoding::byteNumSize(), NumNullColsMissing);

        let columnNum = u32::from_ne_bytes(rowData[0..Encoding::byteNumSize()].try_into().unwrap()) as usize;
        if columnNum > 0 {
            ContiguousRowReaderWithNulls::new(rowData, schema, columnNum).map(Self::WithNulls)
        } else {
            Ok(Self::NoNulls(ContiguousRowReaderNoNulls {
                rowData,
                columnByteOffsetSlice: schema.columnByteOffsetVec(),
                datumOffset: Encoding::byteNumSize(),
            }))
        }
    }
}

impl<'a, T: Deref<Target=[u8]>> ContiguousRow for ContiguousRowReader<'a, T> {
    fn num_datum_views(&self) -> usize {
        match self {
            Self::NoNulls(v) => v.num_datum_views(),
            Self::WithNulls(v) => v.num_datum_views(),
        }
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView {
        match self {
            Self::NoNulls(v) => v.datum_view_at(index, datum_kind),
            Self::WithNulls(v) => v.datum_view_at(index, datum_kind),
        }
    }
}

pub struct ContiguousRowReaderNoNulls<'a, T> {
    rowData: &'a T,
    columnByteOffsetSlice: &'a [usize],
    datumOffset: usize,
}

impl<'a, T: Deref<Target=[u8]>> ContiguousRow for ContiguousRowReaderNoNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.columnByteOffsetSlice.len()
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView<'a> {
        let offset = self.columnByteOffsetSlice[index];
        let datum_buf = &self.rowData[self.datumOffset + offset..];
        datum_view_at(datum_buf, self.rowData, datum_kind)
    }
}

pub struct ContiguousRowReaderWithNulls<'a, T> {
    rowData: &'a T,
    actualColByteOffsetVec: Vec<isize>,
    datumOffset: usize,
}

impl<'a, T: Deref<Target=[u8]>> ContiguousRowReaderWithNulls<'a, T> {
    fn new(rowData: &'a T, schema: &'a Schema, columnNum: usize) -> Result<Self> {
        assert!(columnNum > 0);

        // bitSet中要用到的bit数量和表的column数量相同
        let bitSetSize = BitSet::getByteNum(columnNum);
        let remaining = &rowData[Encoding::byteNumSize()..];
        ensure!(remaining.len() >= bitSetSize, InvalidBitSetBytes {expect_len: bitSetSize, given_len: remaining.len()});

        // 记录column是不是null的
        let bitSet = RoBitSet::new(&rowData[Encoding::byteNumSize()..Encoding::byteNumSize() + bitSetSize], columnNum).unwrap();

        // 事实上schema.num_columns()和columnNum相等
        let mut actualColumnByteOffsetVec = Vec::with_capacity(schema.num_columns());
        // 总共的涉及到null的column的大小
        let mut accumulatedNullColumnByteLen = 0;

        for (columnIndex, columnOffset) in schema.columnByteOffsetVec().iter().enumerate() {
            match bitSet.is_set(columnIndex) {
                // 不是null
                Some(true) => actualColumnByteOffsetVec.push((*columnOffset - accumulatedNullColumnByteLen) as isize),
                Some(false) => {
                    actualColumnByteOffsetVec.push(-1);
                    accumulatedNullColumnByteLen += byteSizeOfDatum(&schema.column(columnIndex).datumKind);
                }
                None => actualColumnByteOffsetVec.push(-1),
            }
        }

        Ok(Self {
            rowData,
            actualColByteOffsetVec: actualColumnByteOffsetVec,
            datumOffset: Encoding::byteNumSize() + bitSetSize,
        })
    }
}

impl<'a, T: Deref<Target=[u8]>> ContiguousRow for ContiguousRowReaderWithNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.actualColByteOffsetVec.len()
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView<'a> {
        let offset = self.actualColByteOffsetVec[index];
        if offset < 0 {
            DatumView::Null
        } else {
            let datum_offset = self.datumOffset + offset as usize;
            let datum_buf = &self.rowData[datum_offset..];
            datum_view_at(datum_buf, self.rowData, datum_kind)
        }
    }
}

fn datum_view_at<'a>(datum_buf: &'a [u8],
                     string_buf: &'a [u8],
                     datum_kind: &DatumKind) -> DatumView<'a> {
    must_read_view(datum_kind, datum_buf, string_buf)
}

/// Contiguous row with projection information.
///
/// The caller must ensure the source schema of projector is the same as the schema of source row.
pub struct ProjectedContiguousRow<'a, T> {
    contiguousRow: T,
    rowProjector: &'a RowProjector,
}

impl<'a, T: ContiguousRow> ProjectedContiguousRow<'a, T> {
    pub fn new(contiguousRow: T, rowProjector: &'a RowProjector) -> Self {
        Self { contiguousRow, rowProjector }
    }

    pub fn datumViewNum(&self) -> usize {
        self.rowProjector.source_projection().len()
    }

    pub fn datumViewAtIndex(&self, index: usize) -> DatumView {
        match self.rowProjector.source_projection()[index] {
            Some(index_in_source) => {
                let datum_kind = self.rowProjector.datum_kind(index_in_source);
                self.contiguousRow.datum_view_at(index_in_source, datum_kind)
            }
            None => DatumView::Null,
        }
    }
}

impl<'a, T: ContiguousRow> fmt::Debug for ProjectedContiguousRow<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for i in 0..self.datumViewNum() {
            let view = self.datumViewAtIndex(i);
            list.entry(&view);
        }
        list.finish()
    }
}

/// In memory buffer to hold data of a contiguous row.
pub trait RowBuffer: DerefMut<Target=[u8]> {
    /// clear and resize the buffer size to `new_len` with given `value`.
    fn reset(&mut self, new_len: usize, value: u8);

    /// Append slice into the buffer, resize the buffer automatically.
    fn append_slice(&mut self, src: &[u8]);
}

/// A writer to build a contiguous row.
pub struct ContiguousRowWriter<'a, T> {
    buffer: &'a mut T,
    /// The schema the row group need to be encoded into, the schema of the row need to be write compatible for the table schema.
    tableSchema: &'a Schema,
    /// The index mapping from table schema to column in the schema of row group.
    indexInWriter: &'a IndexInWriterSchema,
}

// TODO(yingwen): Try to replace usage of row by contiguous row.
impl<'a, T: RowBuffer + 'a> ContiguousRowWriter<'a, T> {
    pub fn new(buffer: &'a mut T,
               tableSchema: &'a Schema,
               indexInWriter: &'a IndexInWriterSchema) -> Self {
        Self {
            buffer,
            tableSchema,
            indexInWriter,
        }
    }

    /// write a row to the buffer
    pub fn writeRow(&mut self, row: &Row) -> Result<()> {
        let mut nullColNum = 0;

        for columnIndexInTable in 0..self.tableSchema.num_columns() {
            // 写入的时候涉及到了
            if let Some(columnIndexInWriter) = self.indexInWriter.columnIndexInWriter(columnIndexInTable) {
                let datum = &row[columnIndexInWriter];
                if datum.is_null() {
                    nullColNum += 1;
                }
            } else { // 写入的时候都未涉及到
                nullColNum += 1;
            }
        }

        if nullColNum > 0 {
            self.writeRowWithNulls(row)
        } else {
            self.writeRowWithoutNulls(row)
        }
    }

    fn writeRowWithNulls(&mut self, row: &Row) -> Result<()> {
        let columnNum = self.tableSchema.num_columns();

        // assume most columns are not null, so use a bitset with all bit set at first.
        let mut nullsBitSet = BitSet::all_set(columnNum);

        // 对应的是打头的4字节 + bitset内容长度
        let mut encodedLen = Encoding::byteNumSize() + nullsBitSet.as_bytes().len();
        let mut num_bytes_of_variable_col = 0;

        for columnIndexInTable in 0..self.tableSchema.num_columns() {
            if let Some(columnIndexInWrite) = self.indexInWriter.columnIndexInWriter(columnIndexInTable) {
                let datum = &row[columnIndexInWrite];

                // no need to store null column.
                if !datum.is_null() {
                    encodedLen += byteSizeOfDatum(&datum.kind());
                }

                if !datum.is_fixed_sized() {
                    let len = datum.size();
                    let size = encoded_len_varint(len as u64) + len;

                    num_bytes_of_variable_col += size;
                    encodedLen += size;
                }
            } else {
                // no need to store null column.
            }
        }

        // pre-allocate the memory.
        self.buffer.reset(encodedLen, 0);

        let mut next_string_offset = encodedLen - num_bytes_of_variable_col;
        let mut datum_offset = Encoding::byteNumSize() + nullsBitSet.as_bytes().len();
        for columnIndexInTable in 0..self.tableSchema.num_columns() {
            if let Some(columnIndexInWrite) = self.indexInWriter.columnIndexInWriter(columnIndexInTable) {
                let datum = &row[columnIndexInWrite];

                Self::writeDatum(self.buffer, datum, &mut datum_offset, &mut next_string_offset)?;

                // insert的本身是null
                if datum.is_null() {
                    nullsBitSet.unset(columnIndexInTable);
                }
            } else { // insert时候未提到
                nullsBitSet.unset(columnIndexInTable);
            }
        }

        // 当insert时候(编码row)有null的时候 头4字节记录全部column数量 a 后边是bitset fenquen
        Self::writeSliceToOffset(self.buffer, &mut 0, &(columnNum as u32).to_ne_bytes());
        Self::writeSliceToOffset(self.buffer, &mut Encoding::byteNumSize(), nullsBitSet.as_bytes());

        debug_assert_eq!(datum_offset, encodedLen - num_bytes_of_variable_col);
        debug_assert_eq!(next_string_offset, encodedLen);

        Ok(())
    }

    fn writeRowWithoutNulls(&mut self, row: &Row) -> Result<()> {
        // 打头的4字节 + 变长数据实际保存地区的起始offset
        let datum_buffer_len = Encoding::byteNumSize() + self.tableSchema.string_buffer_offset();

        // 算到底要用掉的多少byte
        let mut encodedLen = datum_buffer_len;
        for columnIndexInTable in 0..self.tableSchema.num_columns() {
            if let Some(columnIndexInWriter) = self.indexInWriter.columnIndexInWriter(columnIndexInTable) {
                let datum = &row[columnIndexInWriter];

                if !datum.is_fixed_sized() { // for the datum content and the length of it
                    let len = datum.size();
                    encodedLen += encoded_len_varint(len as u64) + len;
                }
            } else {
                unreachable!("the column is ensured to be non-null");
            }
        }

        // pre-allocate memory
        self.buffer.reset(encodedLen, DatumKind::Null.into_u8());

        // 实际的写data
        let mut nextVarLenDataOffset = datum_buffer_len;
        // insert时候的头4字节用来记录全部的column数量当writeRowWithNulls时候会用到写入其它时候不写入 fenquen
        let mut nextDatumOffset = Encoding::byteNumSize();
        for columnIndexInTable in 0..self.tableSchema.num_columns() {
            if let Some(columnIndexInWriter) = self.indexInWriter.columnIndexInWriter(columnIndexInTable) {
                let datum = &row[columnIndexInWriter];
                Self::writeDatum(self.buffer, datum, &mut nextDatumOffset, &mut nextVarLenDataOffset)?;
            } else {
                unreachable!("the column is ensured to be non-null");
            }
        }

        debug_assert_eq!(nextDatumOffset, datum_buffer_len);
        debug_assert_eq!(nextVarLenDataOffset, encodedLen);

        Ok(())
    }

    fn writeDatum(buffer: &mut T,
                  datum: &Datum,
                  offset: &mut usize,
                  nextVarLenDataOffset: &mut usize) -> Result<()> {
        match datum {
            Datum::Null => {}// already filled by null, nothing to do.
            Datum::Timestamp(v) => {
                let value_buf = v.as_i64().to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Double(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Float(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Varbinary(v) => {
                ensure!(*nextVarLenDataOffset <= MAX_ROW_LEN, StringTooLong {len: *nextVarLenDataOffset});

                // 把实际的data的offset写到栏位
                Self::writeSliceToOffset(buffer, offset, &(*nextVarLenDataOffset as u32).to_ne_bytes());

                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let mut buf = [0; 4];
                // 写 长度
                Self::writeSliceToOffset(buffer, nextVarLenDataOffset, Self::encodeVarInt(v.len() as u32, &mut buf));
                // 写 data
                Self::writeSliceToOffset(buffer, nextVarLenDataOffset, v);
            }
            Datum::String(v) => {
                ensure!(*nextVarLenDataOffset <= MAX_ROW_LEN, StringTooLong {len: *nextVarLenDataOffset});

                // 把实际的data的offset写到栏位
                // let value_buf = (*nextVarLenDataOffset as u32).to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &(*nextVarLenDataOffset as u32).to_ne_bytes());

                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let mut buf = [0; 4];
                //let value_buf = Self::encodeVarInt(v.len() as u32, &mut buf);
                // 写 长度
                Self::writeSliceToOffset(buffer, nextVarLenDataOffset, Self::encodeVarInt(v.len() as u32, &mut buf));
                // 写 data
                Self::writeSliceToOffset(buffer, nextVarLenDataOffset, v.as_bytes());
            }
            Datum::UInt64(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::UInt32(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::UInt16(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::UInt8(v) => {
                Self::writeSliceToOffset(buffer, offset, &[*v]);
            }
            Datum::Int64(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Int32(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Int16(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Int8(v) => {
                Self::writeSliceToOffset(buffer, offset, &[*v as u8]);
            }
            Datum::Boolean(v) => {
                Self::writeSliceToOffset(buffer, offset, &[*v as u8]);
            }
            Datum::Date(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
            Datum::Time(v) => {
                let value_buf = v.to_ne_bytes();
                Self::writeSliceToOffset(buffer, offset, &value_buf);
            }
        }

        Ok(())
    }

    #[inline]
    fn writeSliceToOffset(buffer: &mut T, offset: &mut usize, value_buf: &[u8]) {
        let dst = &mut buffer[*offset..*offset + value_buf.len()];
        dst.copy_from_slice(value_buf);
        *offset += value_buf.len();
    }

    fn encodeVarInt(value: u32, buf: &mut [u8; 4]) -> &[u8] {
        let value = value as u64;
        let mut temp = &mut buf[..];
        encode_varint(value, &mut temp);
        &buf[..encoded_len_varint(value)]
    }
}

/// For string types the datum size is the memory size to hold the offset.
pub(crate) fn byteSizeOfDatum(kind: &DatumKind) -> usize {
    match kind {
        DatumKind::Null => 1,
        DatumKind::Timestamp => mem::size_of::<Timestamp>(),
        DatumKind::Double => mem::size_of::<f64>(),
        DatumKind::Float => mem::size_of::<f32>(),
        // The size of offset.
        DatumKind::Varbinary | DatumKind::String => Encoding::offsetSize(),
        DatumKind::UInt64 => mem::size_of::<u64>(),
        DatumKind::UInt32 => mem::size_of::<u32>(),
        DatumKind::UInt16 => mem::size_of::<u16>(),
        DatumKind::UInt8 => mem::size_of::<u8>(),
        DatumKind::Int64 => mem::size_of::<i64>(),
        DatumKind::Int32 => mem::size_of::<i32>(),
        DatumKind::Int16 => mem::size_of::<i16>(),
        DatumKind::Int8 => mem::size_of::<i8>(),
        DatumKind::Boolean => mem::size_of::<bool>(),
        DatumKind::Date => mem::size_of::<i32>(),
        DatumKind::Time => mem::size_of::<i64>(),
    }
}

/// Read datum view from given datum buf, and may reference the string in
/// `string_buf`.
///
/// Panic if out of bound.
///
/// ## Safety
/// The string in buffer must be valid utf8.
fn must_read_view<'a>(
    datum_kind: &DatumKind,
    datum_buf: &'a [u8],
    string_buf: &'a [u8],
) -> DatumView<'a> {
    match datum_kind {
        DatumKind::Null => DatumView::Null,
        DatumKind::Timestamp => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let ts = Timestamp::new(i64::from_ne_bytes(value_buf));
            DatumView::Timestamp(ts)
        }
        DatumKind::Double => {
            let value_buf = datum_buf[..mem::size_of::<f64>()].try_into().unwrap();
            let v = f64::from_ne_bytes(value_buf);
            DatumView::Double(v)
        }
        DatumKind::Float => {
            let value_buf = datum_buf[..mem::size_of::<f32>()].try_into().unwrap();
            let v = f32::from_ne_bytes(value_buf);
            DatumView::Float(v)
        }
        DatumKind::Varbinary => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            DatumView::Varbinary(bytes)
        }
        DatumKind::String => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            let v = unsafe { str::from_utf8_unchecked(bytes) };
            DatumView::String(v)
        }
        DatumKind::UInt64 => {
            let value_buf = datum_buf[..mem::size_of::<u64>()].try_into().unwrap();
            let v = u64::from_ne_bytes(value_buf);
            DatumView::UInt64(v)
        }
        DatumKind::UInt32 => {
            let value_buf = datum_buf[..mem::size_of::<u32>()].try_into().unwrap();
            let v = u32::from_ne_bytes(value_buf);
            DatumView::UInt32(v)
        }
        DatumKind::UInt16 => {
            let value_buf = datum_buf[..mem::size_of::<u16>()].try_into().unwrap();
            let v = u16::from_ne_bytes(value_buf);
            DatumView::UInt16(v)
        }
        DatumKind::UInt8 => DatumView::UInt8(datum_buf[0]),
        DatumKind::Int64 => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Int64(v)
        }
        DatumKind::Int32 => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Int32(v)
        }
        DatumKind::Int16 => {
            let value_buf = datum_buf[..mem::size_of::<i16>()].try_into().unwrap();
            let v = i16::from_ne_bytes(value_buf);
            DatumView::Int16(v)
        }
        DatumKind::Int8 => DatumView::Int8(datum_buf[0] as i8),
        DatumKind::Boolean => DatumView::Boolean(datum_buf[0] != 0),
        DatumKind::Date => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Date(v)
        }
        DatumKind::Time => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Time(v)
        }
    }
}

fn must_read_bytes<'a>(datum_buf: &'a [u8], string_buf: &'a [u8]) -> &'a [u8] {
    // Read offset of string in string buf.
    let value_buf = datum_buf[..Encoding::offsetSize()].try_into().unwrap();
    let offset = u32::from_ne_bytes(value_buf) as usize;
    let mut string_buf = &string_buf[offset..];

    // Read len of the string.
    let string_len = match decode_varint(&mut string_buf) {
        Ok(len) => len as usize,
        Err(e) => panic!("failed to decode string length, string buffer:{string_buf:?}, err:{e}"),
    };

    // Read string.
    &string_buf[..string_len]
}

impl RowBuffer for Vec<u8> {
    fn reset(&mut self, new_len: usize, value: u8) {
        self.clear();
        self.resize(new_len, value);
    }

    fn append_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

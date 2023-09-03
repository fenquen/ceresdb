// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Record batch

use std::{cmp, convert::TryFrom, mem, sync::Arc};

use arrow::{
    array::BooleanArray,
    compute,
    datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch as ArrowRecordBatch,
};
use arrow_ext::operation;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    column::{cast_nanosecond_to_mills, ColumnBlock, ColumnBlockBuilder},
    datum::DatumKind,
    projected_schema::{ProjectedSchema, RowProjector},
    row::{
        contiguous::{ContiguousRow, ProjectedContiguousRow},
        Row, RowViewOnBatch,
    },
    schema::{RecordSchema, RecordSchemaWithKey},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid schema len to build RecordBatch.\nBacktrace:\n{}", backtrace))]
    SchemaLen { backtrace: Backtrace },

    #[snafu(display("Failed to create column block, err:{}", source))]
    CreateColumnBlock { source: crate::column::Error },

    #[snafu(display(
    "Failed to create arrow record batch, err:{}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    CreateArrow {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to iterate datum, err:{}", source))]
    IterateDatum { source: crate::row::Error },

    #[snafu(display("Failed to append datum, err:{}", source))]
    AppendDatum { source: crate::column::Error },

    #[snafu(display(
    "Column not in schema with key, column_name:{}.\nBacktrace:\n{}",
    name,
    backtrace
    ))]
    ColumnNotInSchemaWithKey { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to convert arrow schema, err:{}", source))]
    ConvertArrowSchema { source: crate::schema::Error },

    #[snafu(display("Mismatch record schema to build RecordBatch, column_name:{}, schema_type:{:?}, column_type:{:?}.\nBacktrace:\n{}", column_name, schema_type, column_type, backtrace))]
    MismatchRecordSchema {
        column_name: String,
        schema_type: DatumKind,
        column_type: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Projection is out of the index, source_projection:{:?}, arrow_schema:{}.\nBacktrace:\n{}",
    source_projection,
    arrow_schema,
    backtrace
    ))]
    OutOfIndexProjection {
        source_projection: Vec<Option<usize>>,
        arrow_schema: ArrowSchemaRef,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to reverse record batch data, err:{:?}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    ReverseRecordBatchData {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to select record batch data, err:{:?}.\nBacktrace:\n{}",
    source,
    backtrace
    ))]
    SelectRecordBatchData {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct RecordBatchData {
    pub arrowRecordBatch: ArrowRecordBatch,
    columnBlockVec: Vec<ColumnBlock>,
}

impl RecordBatchData {
    fn new(arrowSchema: ArrowSchemaRef, columnBlockVec: Vec<ColumnBlock>) -> Result<Self> {
        let arrowArrays = columnBlockVec.iter().map(|columnBlock| columnBlock.to_arrow_array_ref()).collect();

        let arrowRecordBatch = ArrowRecordBatch::try_new(arrowSchema, arrowArrays).context(CreateArrow)?;

        Ok(RecordBatchData { arrowRecordBatch, columnBlockVec })
    }

    fn num_rows(&self) -> usize {
        self.columnBlockVec.first().map(|column| column.num_rows()).unwrap_or(0)
    }

    fn take_column_block(&mut self, index: usize) -> ColumnBlock {
        let num_rows = self.num_rows();
        mem::replace(&mut self.columnBlockVec[index], ColumnBlock::new_null(num_rows))
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// Panics if offset with length is greater than column length.
    fn slice(&self, offset: usize, length: usize) -> Self {
        let column_blocks = self
            .columnBlockVec
            .iter()
            .map(|col| col.slice(offset, length))
            .collect();

        Self {
            arrowRecordBatch: self.arrowRecordBatch.slice(offset, length),
            columnBlockVec: column_blocks,
        }
    }
}

fn build_column_blocks_from_arrow_record_batch(
    arrow_record_batch: &ArrowRecordBatch,
    record_schema: &RecordSchema,
) -> Result<Vec<ColumnBlock>> {
    let mut column_blocks = Vec::with_capacity(arrow_record_batch.num_columns());
    for (column_schema, array) in record_schema
        .columns()
        .iter()
        .zip(arrow_record_batch.columns())
    {
        let column = ColumnBlock::try_from_arrow_array_ref(&column_schema.datumKind, array)
            .context(CreateColumnBlock)?;
        column_blocks.push(column);
    }

    Ok(column_blocks)
}

impl TryFrom<ArrowRecordBatch> for RecordBatchData {
    type Error = Error;

    fn try_from(arrow_record_batch: ArrowRecordBatch) -> Result<Self> {
        let record_schema =
            RecordSchema::try_from(arrow_record_batch.schema()).context(ConvertArrowSchema)?;
        let column_blocks =
            build_column_blocks_from_arrow_record_batch(&arrow_record_batch, &record_schema)?;
        Ok(Self {
            arrowRecordBatch: arrow_record_batch,
            columnBlockVec: column_blocks,
        })
    }
}

// TODO(yingwen): The schema in RecordBatch should be much simple because it may lack some information.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    recordSchema: RecordSchema,
    recordBatchData: RecordBatchData,
}

impl RecordBatch {
    pub fn new_empty(schema: RecordSchema) -> Self {
        let arrow_schema = schema.to_arrow_schema_ref();
        let arrow_record_batch = ArrowRecordBatch::new_empty(arrow_schema);

        Self {
            recordSchema: schema,
            recordBatchData: RecordBatchData {
                arrowRecordBatch: arrow_record_batch,
                columnBlockVec: Vec::new(),
            },
        }
    }

    pub fn new(schema: RecordSchema, column_blocks: Vec<ColumnBlock>) -> Result<Self> {
        ensure!(schema.num_columns() == column_blocks.len(), SchemaLen);

        // Validate schema and column_blocks.
        for (column_schema, column_block) in schema.columns().iter().zip(column_blocks.iter()) {
            ensure!(
                column_schema.datumKind == column_block.datum_kind(),
                MismatchRecordSchema {
                    column_name: &column_schema.name,
                    schema_type: column_schema.datumKind,
                    column_type: column_block.datum_kind(),
                }
            );
        }

        let arrow_schema = schema.to_arrow_schema_ref();
        let data = RecordBatchData::new(arrow_schema, column_blocks)?;

        Ok(Self { recordSchema: schema, recordBatchData: data })
    }

    pub fn schema(&self) -> &RecordSchema {
        &self.recordSchema
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    // REQUIRE: index is valid
    #[inline]
    pub fn column(&self, index: usize) -> &ColumnBlock {
        &self.recordBatchData.columnBlockVec[index]
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.recordSchema.num_columns()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.recordBatchData.num_rows()
    }

    #[inline]
    pub fn as_arrow_record_batch(&self) -> &ArrowRecordBatch {
        &self.recordBatchData.arrowRecordBatch
    }

    #[inline]
    pub fn intoArrowRecordBatch(self) -> ArrowRecordBatch {
        self.recordBatchData.arrowRecordBatch
    }
}

impl TryFrom<ArrowRecordBatch> for RecordBatch {
    type Error = Error;

    fn try_from(arrowRecordBatch: ArrowRecordBatch) -> Result<Self> {
        let record_schema =
            RecordSchema::try_from(arrowRecordBatch.schema()).context(ConvertArrowSchema)?;

        let column_blocks =
            build_column_blocks_from_arrow_record_batch(&arrowRecordBatch, &record_schema)?;

        let arrow_record_batch = cast_arrow_record_batch(arrowRecordBatch)?;
        Ok(Self {
            recordSchema: record_schema,
            recordBatchData: RecordBatchData {
                arrowRecordBatch: arrow_record_batch,
                columnBlockVec: column_blocks,
            },
        })
    }
}

fn cast_arrow_record_batch(source: ArrowRecordBatch) -> Result<ArrowRecordBatch> {
    let row_count = source.num_columns();
    if row_count == 0 {
        return Ok(source);
    }
    let columns = source.columns();
    let mut casted_columns = Vec::with_capacity(columns.len());
    for column in columns {
        let column = match column.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                cast_nanosecond_to_mills(column).context(AppendDatum)?
            }
            _ => column.clone(),
        };
        casted_columns.push(column);
    }

    let schema = source.schema();
    let fields = schema.all_fields();
    let mills_fileds = fields
        .iter()
        .map(|field| {
            let mut f = match field.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, None) => Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    field.is_nullable(),
                ),
                _ => {
                    let (dict_id, dict_is_ordered) = {
                        match field.data_type() {
                            DataType::Dictionary(_, _) => {
                                (field.dict_id().unwrap(), field.dict_is_ordered().unwrap())
                            }
                            _ => (0, false),
                        }
                    };
                    Field::new_dict(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                        dict_id,
                        dict_is_ordered,
                    )
                }
            };
            f.set_metadata(field.metadata().clone());
            f
        })
        .collect::<Vec<_>>();
    let mills_schema = Schema {
        fields: mills_fileds.into(),
        metadata: schema.metadata().clone(),
    };
    let result =
        ArrowRecordBatch::try_new(Arc::new(mills_schema), casted_columns).context(CreateArrow)?;
    Ok(result)
}

#[derive(Debug)]
pub struct RecordBatchWithKey {
    recordSchemaWithKey: RecordSchemaWithKey,
    pub recordBatchData: RecordBatchData,
}

impl RecordBatchWithKey {
    pub fn rowCount(&self) -> usize {
        self.recordBatchData.num_rows()
    }

    pub fn num_columns(&self) -> usize {
        self.recordBatchData.arrowRecordBatch.num_columns()
    }

    pub fn columns(&self) -> &[ColumnBlock] {
        &self.recordBatchData.columnBlockVec
    }

    pub fn clone_row_at(&self, index: usize) -> Row {
        let datums = self
            .recordBatchData
            .columnBlockVec
            .iter()
            .map(|column_block| column_block.datum(index))
            .collect();

        Row::from_datums(datums)
    }

    /// Project the [RecordBatchWithKey] into a [RecordBatch] according to
    /// [ProjectedSchema].
    ///
    /// REQUIRE: The schema_with_key of the [RecordBatchWithKey] is the same as
    /// the schema_with_key of [ProjectedSchema].
    pub fn try_project(mut self, projected_schema: &ProjectedSchema) -> Result<RecordBatch> {
        debug_assert_eq!(
            &self.recordSchemaWithKey,
            projected_schema.as_record_schema_with_key()
        );

        // Get the schema after projection.
        let record_schema = projected_schema.to_record_schema();
        let mut column_blocks = Vec::with_capacity(record_schema.num_columns());

        for column_schema in record_schema.columns() {
            let column_index = self.recordSchemaWithKey.index_of(&column_schema.name).context(
                ColumnNotInSchemaWithKey {
                    name: &column_schema.name,
                },
            )?;

            // Take the column block out.
            let column_block = self.recordBatchData.take_column_block(column_index);
            column_blocks.push(column_block);
        }

        let data = RecordBatchData::new(record_schema.to_arrow_schema_ref(), column_blocks)?;

        Ok(RecordBatch {
            recordSchema: record_schema,
            recordBatchData: data,
        })
    }

    pub fn intoRecordBatch(self) -> RecordBatch {
        RecordBatch {
            recordSchema: self.recordSchemaWithKey.recordSchema,
            recordBatchData: self.recordBatchData,
        }
    }

    pub fn as_arrow_record_batch(&self) -> &ArrowRecordBatch {
        &self.recordBatchData.arrowRecordBatch
    }

    #[inline]
    pub fn schema_with_key(&self) -> &RecordSchemaWithKey {
        &self.recordSchemaWithKey
    }

    #[inline]
    pub fn column(&self, index: usize) -> &ColumnBlock {
        &self.recordBatchData.columnBlockVec[index]
    }

    /// Reverse the rows in the data.
    ///
    /// The data retains intact if failed.
    pub fn reverse_data(&mut self) -> Result<()> {
        let reversed_record_batch = operation::reverse_record_batch(&self.recordBatchData.arrowRecordBatch)
            .map_err(|e| Box::new(e) as _)
            .context(ReverseRecordBatchData)?;

        self.recordBatchData = RecordBatchData::try_from(reversed_record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(ReverseRecordBatchData)?;

        Ok(())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rowCount() == 0
    }

    /// Returns a zero-copy slice of this array with the indicated offset and
    /// length.
    ///
    /// Panics if offset with length is greater than column length.
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            recordSchemaWithKey: self.recordSchemaWithKey.clone(),
            recordBatchData: self.recordBatchData.slice(offset, length),
        }
    }

    /// Select the rows according to the `filter_array`.
    pub fn select_data(&mut self, filter_array: &BooleanArray) -> Result<()> {
        assert_eq!(self.rowCount(), filter_array.len());
        let selected_record_batch =
            compute::filter_record_batch(&self.recordBatchData.arrowRecordBatch, filter_array)
                .map_err(|e| Box::new(e) as _)
                .context(SelectRecordBatchData)?;

        self.recordBatchData = RecordBatchData::try_from(selected_record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(SelectRecordBatchData)?;

        Ok(())
    }
}

pub struct RecordBatchWithKeyBuilder {
    recordSchemaWithKey: RecordSchemaWithKey,
    columnBlockBuilderVec: Vec<ColumnBlockBuilder>,
}

impl RecordBatchWithKeyBuilder {
    pub fn new(recordSchemaWithKey: RecordSchemaWithKey) -> Self {
        let columnBlockBuilderVec =
            recordSchemaWithKey.columns().iter().map(|columnSchema| {
                ColumnBlockBuilder::newWithCapacity(&columnSchema.datumKind, 0, columnSchema.is_dictionary)
            }).collect();
        RecordBatchWithKeyBuilder {
            recordSchemaWithKey,
            columnBlockBuilderVec,
        }
    }

    pub fn newWithCapacity(recordSchemaWithKey: RecordSchemaWithKey, capacity: usize) -> Self {
        let columnBlockBuilderVec =
            recordSchemaWithKey.columns().iter().map(|columnSchema| {
                ColumnBlockBuilder::newWithCapacity(&columnSchema.datumKind, capacity, columnSchema.is_dictionary)
            }).collect();
        RecordBatchWithKeyBuilder {
            recordSchemaWithKey,
            columnBlockBuilderVec,
        }
    }

    /// Append row into builder.
    ///
    /// REQUIRE: The row and the builder must have the same schema.
    pub fn append_row(&mut self, row: Row) -> Result<()> {
        for (builder, datum) in self.columnBlockBuilderVec.iter_mut().zip(row) {
            builder.appendDatum(datum).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append projected contiguous row into builder.
    /// REQUIRE:
    /// - The schema of `row` is the same as the source schema of the `projector`.
    /// - The projected schema (with key) is the same as the schema of the builder.
    pub fn appendProjectedContiguousRow<T: ContiguousRow>(&mut self, row: &ProjectedContiguousRow<T>) -> Result<()> {
        assert_eq!(row.datumViewNum(), self.columnBlockBuilderVec.len());

        for (columnIndexInProjection, columnBlockBuilder) in self.columnBlockBuilderVec.iter_mut().enumerate() {
            let datumView = row.datumViewAtIndex(columnIndexInProjection);
            columnBlockBuilder.appendDatumView(datumView).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append the row from the [RowView] to the builder.
    ///
    /// REQUIRE: The `row_view` and the builder must have the same schema.
    pub fn append_row_view(&mut self, row_view: &RowViewOnBatch) -> Result<()> {
        for (builder, datum_view) in self.columnBlockBuilderVec.iter_mut().zip(row_view.iter_columns()) {
            let datum_view = datum_view.context(IterateDatum)?;
            builder.appendDatumView(datum_view).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append `len` from `start` (inclusive) to this builder.
    ///
    /// REQUIRE: the `record_batch` and the builder must have the same schema.
    pub fn append_batch_range(&mut self,
                              record_batch: &RecordBatchWithKey,
                              start: usize,
                              len: usize, ) -> Result<usize> {
        let num_rows = record_batch.rowCount();
        if start >= num_rows {
            return Ok(0);
        }

        let added = cmp::min(num_rows - start, len);

        for (builder, column) in self.columnBlockBuilderVec.iter_mut().zip(record_batch.columns().iter()) {
            builder.append_block_range(column, start, added).context(AppendDatum)?;
        }

        Ok(added)
    }

    /// The number of the appended rows.
    pub fn len(&self) -> usize {
        self.columnBlockBuilderVec.first().map(|builder| builder.len()).unwrap_or(0)
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the builders for reuse.
    pub fn clear(&mut self) {
        for builder in &mut self.columnBlockBuilderVec {
            builder.clear();
        }
    }

    pub fn build(&mut self) -> Result<RecordBatchWithKey> {
        let columnBlockVec: Vec<ColumnBlock> = self.columnBlockBuilderVec.iter_mut().map(|columnBlockBuilder| columnBlockBuilder.build()).collect();
        let arrowSchema = self.recordSchemaWithKey.recordSchema.arrowSchema.clone();

        Ok(RecordBatchWithKey {
            recordSchemaWithKey: self.recordSchemaWithKey.clone(),
            recordBatchData: RecordBatchData::new(arrowSchema, columnBlockVec)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArrowRecordBatchProjector {
    row_projector: RowProjector,
}

impl From<RowProjector> for ArrowRecordBatchProjector {
    fn from(row_projector: RowProjector) -> Self {
        Self { row_projector }
    }
}

impl ArrowRecordBatchProjector {
    /// Project the [arrow::RecordBatch] to [RecordBatchWithKey] and these
    /// things are to be done:
    ///  - Insert the null column if the projected column does not appear in the
    ///    source schema.
    ///  - Convert the [arrow::RecordBatch] to [RecordBatchWithKey].
    ///
    /// REQUIRE: Schema of the `arrow_record_batch` is the same as the
    /// projection of existing column in the source schema.
    pub fn project_to_record_batch_with_key(
        &self,
        arrow_record_batch: ArrowRecordBatch,
    ) -> Result<RecordBatchWithKey> {
        let schema_with_key = self.row_projector.schema_with_key().clone();
        let source_projection = self.row_projector.source_projection();
        let mut column_blocks = Vec::with_capacity(schema_with_key.num_columns());

        let num_rows = arrow_record_batch.num_rows();
        // ensure next_arrow_column_idx < num_columns
        let mut next_arrow_column_idx = 0;
        let num_columns = arrow_record_batch.num_columns();

        for (source_idx, column_schema) in source_projection.iter().zip(schema_with_key.columns()) {
            match source_idx {
                Some(_) => {
                    ensure!(
                        next_arrow_column_idx < num_columns,
                        OutOfIndexProjection {
                            source_projection,
                            arrow_schema: arrow_record_batch.schema()
                        }
                    );

                    let array = arrow_record_batch.column(next_arrow_column_idx);
                    next_arrow_column_idx += 1;

                    let column_block =
                        ColumnBlock::try_from_arrow_array_ref(&column_schema.datumKind, array)
                            .context(CreateColumnBlock)?;

                    column_blocks.push(column_block);
                }
                None => {
                    // Need to push row with specific type.
                    let null_block = ColumnBlock::new_null_with_type(
                        &column_schema.datumKind,
                        num_rows,
                        column_schema.is_dictionary,
                    )
                        .context(CreateColumnBlock)?;
                    column_blocks.push(null_block);
                }
            }
        }

        let data = RecordBatchData::new(schema_with_key.to_arrow_schema_ref(), column_blocks)?;

        Ok(RecordBatchWithKey {
            recordSchemaWithKey: schema_with_key,
            recordBatchData: data,
        })
    }
}
// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for insert statement

use std::{
    collections::{BTreeMap, HashMap},
    ops::IndexMut,
    sync::Arc,
};

use arrow::{array::ArrayRef, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use codec::{compact::MemCompactEncoder, Encoder};
use common_types::{
    column::{ColumnBlock, ColumnBlockBuilder},
    column_schema::ColumnId,
    datum::Datum,
    row::RowGroup,
};
use datafusion::{
    common::ToDFSchema,
    error::DataFusionError,
    logical_expr::{expr::Expr as DfLogicalExpr, ColumnarValue as DfColumnarValue},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::{
        create_physical_expr, execution_props::ExecutionProps, expressions::TryCastExpr,
    },
};
use df_operator::visitor::find_columns_by_expr;
use hash_ext::hash64;
use macros::define_result;
use query_frontend::plan::InsertPlan;
use snafu::{OptionExt, ResultExt, Snafu};
use table_engine::table::{TableRef, WriteRequest};

use crate::{
    context::InterpreterContext,
    interpreter::{Insert, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to generate datafusion expr, err:{}", source))]
    DatafusionExpr { source: DataFusionError },

    #[snafu(display(
    "Failed to get data type from datafusion physical expr, err:{}",
    source
    ))]
    DatafusionDataType { source: DataFusionError },

    #[snafu(display("Failed to get arrow schema, err:{}", source))]
    ArrowSchema { source: ArrowError },

    #[snafu(display("Failed to get datafusion schema, err:{}", source))]
    DatafusionSchema { source: DataFusionError },

    #[snafu(display("Failed to evaluate datafusion physical expr, err:{}", source))]
    DatafusionExecutor { source: DataFusionError },

    #[snafu(display("Failed to build arrow record batch, err:{}", source))]
    BuildArrowRecordBatch { source: ArrowError },

    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: table_engine::table::Error },

    #[snafu(display("Failed to encode tsid, err:{}", source))]
    EncodeTsid { source: codec::compact::Error },

    #[snafu(display("Failed to convert arrow array to column block, err:{}", source))]
    ConvertColumnBlock { source: common_types::column::Error },

    #[snafu(display("Failed to find input columns of expr, column_name:{}", column_name))]
    FindExpressionInput { column_name: String },

    #[snafu(display("Failed to build column block, err:{}", source))]
    BuildColumnBlock { source: common_types::column::Error },
}

define_result!(Error);

pub struct InsertInterpreter {
    ctx: InterpreterContext,
    insertPlan: InsertPlan,
}

#[async_trait]
impl Interpreter for InsertInterpreter {
    async fn execute(mut self: Box<Self>) -> InterpreterResult<Output> {
        // generate tsid if needed.
        self.generateTsId().context(Insert)?;

        let InsertPlan {
            table,
            rowGroup: mut rowGroup,
            columnIndex_defaultValue: columnIndex_defaultVal,
        } = self.insertPlan;

        // fill default values
        fill_default_values(table.clone(),
                            &mut rowGroup,
                            &columnIndex_defaultVal).context(Insert)?;

        // context is unused now
        let _ctx = self.ctx;

        let writeRequest = WriteRequest { rowGroup };

        let num_rows = table.write(writeRequest).await.context(WriteTable).context(Insert)?;

        Ok(Output::AffectedRows(num_rows))
    }
}

impl InsertInterpreter {
    pub fn create(ctx: InterpreterContext, insertPlan: InsertPlan) -> InterpreterPtr {
        Box::new(Self { ctx, insertPlan })
    }

    fn generateTsId(&mut self) -> Result<()> {
        let schema = self.insertPlan.rowGroup.schema();

        if let Some(tsIdColumnIndex) = schema.index_of_tsid() {
            // vec of (`index of tag`, `column id of tag`).
            let tagColumnIndex_tagColumnId: Vec<_> = schema.columns().iter().enumerate()
                .filter_map(|(i, column)| {
                    if column.is_tag {
                        Some((i, column.id))
                    } else {
                        None
                    }
                }).collect();

            let mut hash_bytes = Vec::new();

            for i in 0..self.insertPlan.rowGroup.num_rows() {
                let row = self.insertPlan.rowGroup.get_row_mut(i).unwrap();

                let mut tsid_builder = TsidBuilder::new(&mut hash_bytes);

                for (tagColumnIndex, tagColumnId) in &tagColumnIndex_tagColumnId {
                    tsid_builder.writeDatum(*tagColumnId, &row[*tagColumnIndex])?;
                    // fenquen tsid的生成包含tag列的columnId和value
                }

                row[tsIdColumnIndex] = Datum::UInt64(tsid_builder.finish());
            }
        }
        Ok(())
    }
}

struct TsidBuilder<'a> {
    memCompactEncoder: MemCompactEncoder,
    hashByteVec: &'a mut Vec<u8>,
}

impl<'a> TsidBuilder<'a> {
    fn new(hash_bytes: &'a mut Vec<u8>) -> Self {
        // Clear the bytes buffer.
        hash_bytes.clear();

        Self {
            memCompactEncoder: MemCompactEncoder,
            hashByteVec: hash_bytes,
        }
    }

    fn writeDatum(&mut self, tagColumnId: ColumnId, datum: &Datum) -> Result<()> {
        // Null datum will be ignored, so tsid remains unchanged after adding a null column
        if datum.is_null() {
            return Ok(());
        }

        // column id
        self.memCompactEncoder.encode(self.hashByteVec, &Datum::UInt64(u64::from(tagColumnId))).context(EncodeTsid)?;

        // datum
        self.memCompactEncoder.encode(self.hashByteVec, datum).context(EncodeTsid)?;

        Ok(())
    }

    fn finish(self) -> u64 {
        hash64(self.hashByteVec)
    }
}

/// fill missing columns which can be calculated via default value expr.
fn fill_default_values(table: TableRef,
                       row_groups: &mut RowGroup,
                       columnIndex_columnDefaultVal: &BTreeMap<usize, DfLogicalExpr>) -> Result<()> {
    let mut cached_column_values: HashMap<usize, DfColumnarValue> = HashMap::new();

    let table_arrow_schema = table.schema().to_arrow_schema_ref();
    let df_schema_ref = table_arrow_schema
        .clone()
        .to_dfschema_ref()
        .context(DatafusionSchema)?;

    for (column_idx, default_value_expr) in columnIndex_columnDefaultVal.iter() {
        let execution_props = ExecutionProps::default();

        // Optimize logical expr
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&execution_props).with_schema(df_schema_ref.clone()),
        );
        let default_value_expr = simplifier
            .coerce(default_value_expr.clone(), df_schema_ref.clone())
            .context(DatafusionExpr)?;
        let simplified_expr = simplifier
            .simplify(default_value_expr)
            .context(DatafusionExpr)?;

        // Find input columns
        let required_column_idxes = find_columns_by_expr(&simplified_expr)
            .iter()
            .map(|column_name| {
                table
                    .schema()
                    .index_of(column_name)
                    .context(FindExpressionInput { column_name })
            })
            .collect::<Result<Vec<usize>>>()?;
        let input_arrow_schema = table_arrow_schema
            .project(&required_column_idxes)
            .context(ArrowSchema)?;
        let input_df_schema = input_arrow_schema
            .clone()
            .to_dfschema()
            .context(DatafusionSchema)?;

        // Create physical expr
        let physical_expr =
            create_physical_expr(&simplified_expr,
                                 &input_df_schema,
                                 &input_arrow_schema,
                                 &execution_props).context(DatafusionExpr)?;

        let from_type = physical_expr
            .data_type(&input_arrow_schema)
            .context(DatafusionDataType)?;
        let to_type = row_groups.schema().column(*column_idx).datumKind;

        let casted_physical_expr = if from_type != to_type.into() {
            Arc::new(TryCastExpr::new(physical_expr, to_type.into()))
        } else {
            physical_expr
        };

        // Build input record batch
        let input_arrays = required_column_idxes
            .into_iter()
            .map(|col_idx| {
                get_or_extract_column_from_row_groups(
                    col_idx,
                    row_groups,
                    &mut cached_column_values,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let inputRecordBatch = if input_arrays.is_empty() {
            RecordBatch::new_empty(Arc::new(input_arrow_schema))
        } else {
            RecordBatch::try_new(Arc::new(input_arrow_schema), input_arrays).context(BuildArrowRecordBatch)?
        };

        let output = casted_physical_expr
            .evaluate(&inputRecordBatch)
            .context(DatafusionExecutor)?;

        fill_column_to_row_group(*column_idx, &output, row_groups)?;

        // Write output to cache.
        cached_column_values.insert(*column_idx, output);
    }

    Ok(())
}

fn fill_column_to_row_group(
    column_idx: usize,
    column: &DfColumnarValue,
    rows: &mut RowGroup,
) -> Result<()> {
    match column {
        DfColumnarValue::Array(array) => {
            let datum_kind = rows.schema().column(column_idx).datumKind;
            let column_block = ColumnBlock::try_from_arrow_array_ref(&datum_kind, array)
                .context(ConvertColumnBlock)?;
            for row_idx in 0..rows.num_rows() {
                let datum = column_block.datum(row_idx);
                rows.get_row_mut(row_idx)
                    .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
            }
        }
        DfColumnarValue::Scalar(scalar) => {
            if let Some(datum) = Datum::from_scalar_value(scalar) {
                for row_idx in 0..rows.num_rows() {
                    rows.get_row_mut(row_idx)
                        .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
                }
            }
        }
    };

    Ok(())
}

/// This method is used to get specific column data.
/// There are two pathes:
///  1. get from cached_column_values
///  2. extract from row_groups
///
/// For performance reasons, we cached the columns extracted from row_groups
/// before, and we will also cache the output of the exprs.
fn get_or_extract_column_from_row_groups(
    column_idx: usize,
    row_groups: &RowGroup,
    cached_column_values: &mut HashMap<usize, DfColumnarValue>,
) -> Result<ArrayRef> {
    let num_rows = row_groups.num_rows();
    let column = cached_column_values
        .get(&column_idx)
        .map(|c| Ok(c.clone()))
        .unwrap_or_else(|| {
            let data_type = row_groups.schema().column(column_idx).datumKind;
            let iter = row_groups.iter_column(column_idx);
            let mut builder = ColumnBlockBuilder::with_capacity(
                &data_type,
                iter.size_hint().0,
                row_groups.schema().column(column_idx).is_dictionary,
            );

            for datum in iter {
                builder.append(datum.clone()).context(BuildColumnBlock)?;
            }

            let columnar_value = DfColumnarValue::Array(builder.build().to_arrow_array_ref());
            cached_column_values.insert(column_idx, columnar_value.clone());
            Ok(columnar_value)
        })?;

    Ok(column.into_array(num_rows))
}

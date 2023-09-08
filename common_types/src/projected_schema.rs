// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Projected schema

use std::{fmt, sync::Arc};

use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    column_schema::{ColumnSchema, ReadOp},
    datum::{Datum, DatumKind},
    row::Row,
    schema::{ArrowSchemaRef, RecordSchema, RecordSchemaWithKey, Schema},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
    "Invalid projection index, index:{}.\nBacktrace:\n{}",
    index,
    backtrace
    ))]
    InvalidProjectionIndex { index: usize, backtrace: Backtrace },

    #[snafu(display("Incompatible column schema for read, err:{}", source))]
    IncompatReadColumn {
        source: crate::column_schema::CompatError,
    },

    #[snafu(display("Failed to build projected schema, err:{}", source))]
    BuildProjectedSchema { source: crate::schema::Error },

    #[snafu(display("Missing not null column for read, name:{}.\nBacktrace:\n{}", name, backtrace))]
    MissingReadColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct RowProjector {
    pub recordSchemaWithKey: RecordSchemaWithKey,
    pub sourceSchema: Schema,
    /// column在投影中的index -> column在table中的index的 fenquen
    /// The Vec stores the column index in source, and `None` means this column is not in source but required by reader, and need to filled by null.
    /// The length of Vec is the same as the number of columns reader intended to read.
    pub sourceProjection: Vec<Option<usize>>,
}

impl RowProjector {
    /// The projected indexes of existed columns in the source schema.
    pub fn existed_source_projection(&self) -> Vec<usize> {
        self.sourceProjection.iter().filter_map(|index| *index).collect()
    }

    /// The projected indexes of all columns(existed and not exist) in the source schema.
    pub fn source_projection(&self) -> &[Option<usize>] {
        &self.sourceProjection
    }

    /// REQUIRE: The schema of row is the same as source schema.
    pub fn project_row(&self, row: &Row, mut datums: Vec<Datum>) -> Row {
        assert_eq!(self.sourceSchema.num_columns(), row.num_columns());

        datums.reserve(self.recordSchemaWithKey.num_columns());

        for p in &self.sourceProjection {
            let datum = match p {
                Some(index_in_source) => row[*index_in_source].clone(),
                None => Datum::Null,
            };

            datums.push(datum);
        }

        Row::from_datums(datums)
    }

    /// return a datum kind selected using an index into the source schema columns.
    pub fn datumKindAtIndex(&self, index: usize) -> &DatumKind {
        assert!(index < self.sourceSchema.num_columns());

        &self.sourceSchema.column(index).datumKind
    }
}

#[derive(Clone)]
pub struct ProjectedSchema(pub Arc<ProjectedSchemaInner>);

impl fmt::Debug for ProjectedSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectedSchema")
            .field("original_schema", &self.0.originalSchema)
            .field("projection", &self.0.projection)
            .finish()
    }
}

impl ProjectedSchema {
    pub fn no_projection(schema: Schema) -> Self {
        let inner = ProjectedSchemaInner::no_projection(schema);
        Self(Arc::new(inner))
    }

    pub fn new(schema: Schema, projection: Option<Vec<usize>>) -> Result<Self> {
        let inner = ProjectedSchemaInner::new(schema, projection)?;
        Ok(Self(Arc::new(inner)))
    }

    pub fn is_all_projection(&self) -> bool {
        self.0.is_all_projection()
    }

    pub fn projection(&self) -> Option<Vec<usize>> {
        self.0.projection()
    }

    /// returns the [RowProjector] to project the rows with source schema to rows with [RecordSchemaWithKey].
    ///
    /// REQUIRE: The key columns are the same as this schema.
    #[inline]
    pub fn tryProjectWithKey(&self, source_schema: &Schema) -> Result<RowProjector> {
        self.0.try_project_with_key(source_schema)
    }

    // Returns the record schema after projection with key.
    pub fn to_record_schema_with_key(&self) -> RecordSchemaWithKey {
        self.0.recordSchemaWithKey.clone()
    }

    pub fn as_record_schema_with_key(&self) -> &RecordSchemaWithKey {
        &self.0.recordSchemaWithKey
    }

    // Returns the record schema after projection.
    pub fn to_record_schema(&self) -> RecordSchema {
        self.0.record_schema.clone()
    }

    /// Returns the arrow schema after projection.
    pub fn to_projected_arrow_schema(&self) -> ArrowSchemaRef {
        self.0.record_schema.to_arrow_schema_ref()
    }
}

impl From<ProjectedSchema> for ceresdbproto::schema::ProjectedSchema {
    fn from(request: ProjectedSchema) -> Self {
        let table_schema_pb = (&request.0.originalSchema).into();
        let projection_pb = request.0.projection.as_ref().map(|project| {
            let project = project.iter().map(|one_project| *one_project as u64).collect::<Vec<u64>>();
            ceresdbproto::schema::Projection { idx: project }
        });

        Self {
            table_schema: Some(table_schema_pb),
            projection: projection_pb,
        }
    }
}

impl TryFrom<ceresdbproto::schema::ProjectedSchema> for ProjectedSchema {
    type Error = Error;

    fn try_from(pb: ceresdbproto::schema::ProjectedSchema) -> std::result::Result<Self, Self::Error> {
        let schema: Schema = pb.table_schema.context(EmptyTableSchema)?.try_into().map_err(|e| Box::new(e) as _).context(ConvertTableSchema)?;
        let projection = pb.projection.map(|v| v.idx.into_iter().map(|id| id as usize).collect());

        ProjectedSchema::new(schema, projection)
    }
}

/// Schema with projection informations
pub struct ProjectedSchemaInner {
    /// The schema before projection that the reader intended to read, may
    /// differ from current schema of the table.
    originalSchema: Schema,
    /// Index of the projected columns in `self.schema`, `None` if
    /// all columns are needed.
    projection: Option<Vec<usize>>,

    /// The record schema from `self.schema` with key columns after projection.
    pub recordSchemaWithKey: RecordSchemaWithKey,
    /// The record schema from `self.schema` after projection.
    record_schema: RecordSchema,
}

impl ProjectedSchemaInner {
    fn no_projection(schema: Schema) -> Self {
        let schema_with_key = schema.to_record_schema_with_key();
        let record_schema = schema.to_record_schema();

        Self {
            originalSchema: schema,
            projection: None,
            recordSchemaWithKey: schema_with_key,
            record_schema,
        }
    }

    fn new(schema: Schema, projection: Option<Vec<usize>>) -> Result<Self> {
        if let Some(p) = &projection {
            // projection is provided, validate the projection is valid. This is necessary
            // to avoid panic when creating RecordSchema and RecordSchemaWithKey.
            if let Some(max_idx) = p.iter().max() {
                ensure!(*max_idx < schema.num_columns(),InvalidProjectionIndex { index: *max_idx });
            }

            let schema_with_key = schema.project_record_schema_with_key(p);
            let record_schema = schema.project_record_schema(p);

            Ok(Self {
                originalSchema: schema,
                projection,
                recordSchemaWithKey: schema_with_key,
                record_schema,
            })
        } else {
            Ok(Self::no_projection(schema))
        }
    }

    /// Selecting all the columns is the all projection.
    fn is_all_projection(&self) -> bool {
        self.projection.is_none()
    }

    fn projection(&self) -> Option<Vec<usize>> {
        self.projection.clone()
    }

    // TODO(yingwen): We can fill missing not null column with default value instead of returning error.
    fn try_project_with_key(&self, source_schema: &Schema) -> Result<RowProjector> {
        debug_assert_eq!(self.recordSchemaWithKey.key_columns(), source_schema.key_columns());

        // We consider the two schema is equal if they have same version.
        if self.originalSchema.version() == source_schema.version() {
            debug_assert_eq!(self.originalSchema, *source_schema);
        }

        //  这样的表(`name` string TAG, `value` double  NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t))的column实际的排列是 tsid,t,name,value fenquen
        let mut sourceProjection = Vec::with_capacity(self.recordSchemaWithKey.num_columns());

        // for each column in `schema_with_key`
        // 即使只select 1个的column 投影的schema中也会是 tsid,t,目标的column fenquen
        for columnSchema in self.recordSchemaWithKey.columns() {
            println!("{}", columnSchema.name);
            self.try_project_column(columnSchema, source_schema, &mut sourceProjection)?;
        }

        Ok(RowProjector {
            recordSchemaWithKey: self.recordSchemaWithKey.clone(),
            sourceSchema: source_schema.clone(),
            sourceProjection,
        })
    }

    fn try_project_column(&self,
                          columnSchema: &ColumnSchema,
                          tableSchema: &Schema,
                          sourceProjection: &mut Vec<Option<usize>>) -> Result<()> {
        match tableSchema.index_of(&columnSchema.name) {
            Some(columnIndexInTable) => {
                // same version, just use that column in source
                if self.originalSchema.version == tableSchema.version {
                    sourceProjection.push(Some(columnIndexInTable));
                } else {  // different version, need to check column schema

                    // 这里用对的情况是column名字相同然而类型变了
                    // 找到当前占用这个index的column
                    let source_column = tableSchema.column(columnIndexInTable);

                    // TODO(yingwen): Data type is not checked here because we do not support alter data type now
                    match columnSchema.compatible_for_read(source_column).context(IncompatReadColumn)? {
                        ReadOp::Exact => sourceProjection.push(Some(columnIndexInTable)),
                        ReadOp::FillNull => sourceProjection.push(None),
                    }
                }
            }
            None => {
                // column is not in source
                ensure!(columnSchema.is_nullable, MissingReadColumn { name: &columnSchema.name });

                // column is nullable, fill this column by null
                sourceProjection.push(None);
            }
        }

        Ok(())
    }
}

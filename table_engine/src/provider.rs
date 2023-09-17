// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Datafusion `TableProvider` adapter

use std::{
    any::Any,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema};
use datafusion::{
    config::{ConfigEntry, ConfigExtension, ExtensionOptions},
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{Count, MetricValue, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use df_operator::visitor;
use log::debug;
use trace_metric::{collector::FormatCollectorVisitor, MetricsCollector};

use crate::{
    predicate::PredicateBuilder,
    stream::{SendableRecordBatchStream, ToDfStream},
    table::{self, ReadOptions, ReadRequest, TableRef},
};
use crate::predicate::Predicate;

const SCAN_TABLE_METRICS_COLLECTOR_NAME: &str = "scan_table";

#[derive(Clone, Debug)]
pub struct CeresdbOptions {
    pub request_id: u64,
    pub request_timeout: Option<u64>,
}

impl ConfigExtension for CeresdbOptions {
    const PREFIX: &'static str = "ceresdb";
}

impl ExtensionOptions for CeresdbOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "request_id" => {
                self.request_id = value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(format!("could not parse request_id, input:{value}, err:{e:?}").into())
                })?
            }
            "request_timeout" => {
                self.request_timeout = Some(value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(format!("could not parse request_timeout, input:{value}, err:{e:?}").into())
                })?)
            }
            _ => Err(DataFusionError::External(
                format!("could not find key, key:{key}").into(),
            ))?,
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: "request_id".to_string(),
                value: Some(self.request_id.to_string()),
                description: "",
            },
            ConfigEntry {
                key: "request_timeout".to_string(),
                value: self.request_timeout.map(|v| v.to_string()),
                description: "",
            },
        ]
    }
}

/// An adapter to [TableProvider] with schema snapshot.
///
/// This adapter holds a schema snapshot of the table and always returns that schema to caller.
#[derive(Debug)]
pub struct TableProviderImpl {
    table: TableRef,
    /// The schema of the table when this adapter is created, used as schema snapshot for read to avoid the reader sees different schema during query
    read_schema: Schema,
}

impl TableProviderImpl {
    pub fn new(table: TableRef) -> Self {
        // Take a snapshot of the schema
        let read_schema = table.schema();
        Self { table, read_schema }
    }

    pub fn as_table_ref(&self) -> &TableRef {
        &self.table
    }

    pub async fn scanTable(&self,
                           dfSessionState: &SessionState,
                           projection: Option<&Vec<usize>>,
                           filters: &[Expr], // 下落到tablescan的where
                           limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        let ceresdbOptions = dfSessionState.config_options().extensions.get::<CeresdbOptions>().unwrap();
        let request_id = RequestId::from(ceresdbOptions.request_id);
        let deadline = ceresdbOptions.request_timeout.map(|n| Instant::now() + Duration::from_millis(n));
        let read_parallelism = dfSessionState.config().target_partitions();

        debug!("scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, deadline:{:?}, parallelism:{}",
            self.table.name(),request_id,projection,filters,limit,deadline,read_parallelism,);

        let predicate = self.buildPredicate(filters);

        let mut scanTable = ScanTable {
            projected_schema: ProjectedSchema::new(self.read_schema.clone(), projection.cloned()).map_err(|e| { DataFusionError::Internal(format!("invalid projection, plan:{self:?}, projection:{projection:?}, err:{e:?}")) })?,
            table: self.table.clone(),
            request_id,
            read_parallelism,
            predicate,
            deadline,
            stream_state: Mutex::new(ScanStreamState::default()),
            metrics_collector: MetricsCollector::new(SCAN_TABLE_METRICS_COLLECTOR_NAME.to_string()),
        };

        scanTable.initStream(dfSessionState).await?;

        Ok(Arc::new(scanTable))
    }

    fn buildPredicate(&self, filters: &[Expr]) -> Arc<Predicate> {
        let uniqueKeyColumnNames = self.read_schema.unique_keys();

        let push_down_filters = filters.iter().filter_map(|filter| {
            if Self::only_filter_unique_key_columns(filter, &uniqueKeyColumnNames) {
                Some(filter.clone())
            } else {
                None
            }
        }).collect::<Vec<_>>();

        PredicateBuilder::default()
            .add_pushdown_exprs(&push_down_filters)
            .extract_time_range(&self.read_schema, &push_down_filters)
            .build()
    }

    // 如果说落地到tablescan的filter的多个column要是有不是表的unique key,那么 the`filter` shouldn't be pushed down.
    fn only_filter_unique_key_columns(filter: &Expr, unique_keys: &[&str]) -> bool {
        let filterColumnNames = visitor::find_columns_by_expr(filter);
        for filterColumnName in filterColumnNames {
            if !unique_keys.contains(&filterColumnName.as_str()) {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl TableProvider for TableProviderImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // We use the `read_schema` as the schema of this `TableProvider`
        self.read_schema.clone().into_arrow_schema_ref()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self,
                  dataFusionSessionState: &SessionState,
                  projection: Option<&Vec<usize>>,// // 投影的index -> colunm真实的在table的index
                  filters: &[Expr],
                  limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        self.scanTable(dataFusionSessionState, projection, filters, limit).await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

impl TableSource for TableProviderImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.read_schema.clone().into_arrow_schema_ref()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tests whether the table provider can make use of a filter expression to optimize data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Default)]
struct ScanStreamState {
    inited: bool,
    err: Option<table::Error>,
    streams: Vec<Option<SendableRecordBatchStream>>,
}

impl ScanStreamState {
    fn take_stream(&mut self, index: usize) -> Result<SendableRecordBatchStream> {
        if let Some(e) = &self.err {
            return Err(DataFusionError::Execution(format!("failed to read table, partition:{index}, err:{e}")));
        }

        // TODO(yingwen): Return an empty stream if index is out of bound.
        self.streams[index].take().ok_or_else(|| {
            DataFusionError::Execution(format!("read partition multiple times is not supported, partition:{index}"))
        })
    }
}

/// dataFusion 的 ExecutionPlan 的自实现类用来 scan table
struct ScanTable {
    projected_schema: ProjectedSchema,
    table: TableRef,
    request_id: RequestId,
    read_parallelism: usize,
    predicate: Arc<Predicate>,
    deadline: Option<Instant>,
    metrics_collector: MetricsCollector,

    stream_state: Mutex<ScanStreamState>,
}

impl ScanTable {
    async fn initStream(&mut self, dfSessionState: &SessionState) -> Result<()> {
        let readRequest = ReadRequest {
            request_id: self.request_id,
            readOptions: ReadOptions {
                batch_size: dfSessionState.config_options().execution.batch_size,
                read_parallelism: self.read_parallelism,
                deadline: self.deadline,
            },
            projectedSchema: self.projected_schema.clone(),
            predicate: self.predicate.clone(),
            metrics_collector: self.metrics_collector.clone(),
        };

        let partitionedStreams = self.table.partitionedRead(readRequest).await;

        let mut scanStreamState = self.stream_state.lock().unwrap();

        if scanStreamState.inited {
            return Ok(());
        }

        match partitionedStreams {
            Ok(partitionedStreams) => {
                self.read_parallelism = partitionedStreams.streams.len();
                scanStreamState.streams = partitionedStreams.streams.into_iter().map(Some).collect();
            }
            Err(e) => scanStreamState.err = Some(e),
        }

        scanStreamState.inited = true;

        Ok(())
    }
}

impl ExecutionPlan for ScanTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::RoundRobinBatch(self.read_parallelism)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    // scanTable依然是最底部的了不会有children
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!("Children cannot be replaced in {self:?}")))
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<DfSendableRecordBatchStream> {
        let stream = self.stream_state.lock().unwrap().take_stream(partition)?;
        Ok(Box::pin(ToDfStream(stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut format_visitor = FormatCollectorVisitor::default();
        self.metrics_collector.visit(&mut format_visitor);
        let metrics_desc = format_visitor.into_string();

        let metric_value = MetricValue::Count {
            name: format!("\n{metrics_desc}").into(),
            count: Count::new(),
        };
        let metric = Metric::new(metric_value, None);
        let mut metric_set = MetricsSet::new();
        metric_set.push(Arc::new(metric));

        Some(metric_set)
    }

    fn statistics(&self) -> Statistics {
        // TODO(yingwen): Implement this
        Statistics::default()
    }
}

impl DisplayAs for ScanTable {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scanTable: table={}, parallelism={}", self.table.name(), self.read_parallelism)
    }
}

impl fmt::Debug for ScanTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanTable")
            .field("projected_schema", &self.projected_schema)
            .field("table", &self.table.name())
            .field("read_parallelism", &self.read_parallelism)
            .field("predicate", &self.predicate).finish()
    }
}
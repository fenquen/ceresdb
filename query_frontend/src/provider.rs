// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Adapter to providers in datafusion

use std::{any::Any, borrow::Cow, cell::RefCell, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use catalog::manager::ManagerRef;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogProvider},
    common::DataFusionError,
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    logical_expr::TableSource,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};
use df_operator::{registry::FunctionRegistry, scalar::ScalarUdf, udaf::AggregateUdf};
use macros::define_result;
use snafu::{OptionExt, ResultExt, Snafu};
use table_engine::{provider::TableProviderImpl, table::TableRef};

use crate::container::{TableContainer, TableReference};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to find catalog, name:{}, err:{}", name, source))]
    FindCatalog {
        name: String,
        source: catalog::manager::Error,
    },

    #[snafu(display("Failed to find schema, name:{}, err:{}", name, source))]
    FindSchema {
        name: String,
        source: catalog::Error,
    },

    #[snafu(display("Failed to find catalog, name:{}", name))]
    CatalogNotFound { name: String },

    #[snafu(display("Failed to find schema, name:{}", name))]
    SchemaNotFound { name: String },

    #[snafu(display("Failed to find table, name:{}, err:{}", name, source))]
    FindTable {
        name: String,
        source: Box<catalog::schema::Error>,
    },

    #[snafu(display(
    "Failed to get all tables, catalog_name:{}, schema_name:{}, err:{}",
    catalog_name,
    schema_name,
    source
    ))]
    GetAllTables {
        catalog_name: String,
        schema_name: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to find udf, err:{}", source))]
    FindUdf {
        source: df_operator::registry::Error,
    },
}

define_result!(Error);

/// MetaProvider provides meta info needed by Frontend
pub trait MetaProvider {
    /// Default catalog name
    fn default_catalog_name(&self) -> &str;

    /// Default schema name
    fn default_schema_name(&self) -> &str;

    /// Get table meta by table reference
    ///
    /// Note that this function may block current thread. We can't make this
    /// function async as the underlying (aka. datafusion) planner needs a sync provider.
    fn table(&self, name: TableReference) -> Result<Option<TableRef>>;

    /// Get udf by name.
    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>>;

    /// Get udaf by name.
    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>>;

    /// Return all tables.
    ///
    /// Note that it may incur expensive cost.
    /// Now it is used in `table_names` method in `SchemaProvider`(introduced by
    /// influxql).
    fn all_tables(&self) -> Result<Vec<TableRef>>;
}

/// We use an adapter instead of using [catalog::Manager] directly, because
/// - MetaProvider provides blocking method, but catalog::Manager may provide
/// async method
/// - Other meta data like default catalog and schema are needed
// TODO(yingwen): Maybe support schema searching instead of using a fixed default schema
pub struct CatalogMetaProvider<'a> {
    pub manager: ManagerRef,
    pub default_catalog: &'a str,
    pub default_schema: &'a str,
    pub function_registry: &'a (dyn FunctionRegistry + Send + Sync),
}

impl<'a> MetaProvider for CatalogMetaProvider<'a> {
    fn default_catalog_name(&self) -> &str {
        self.default_catalog
    }

    fn default_schema_name(&self) -> &str {
        self.default_schema
    }

    fn table(&self, name: TableReference) -> Result<Option<TableRef>> {
        let resolved = name.resolve(self.default_catalog, self.default_schema);

        let catalog = match self
            .manager
            .catalog_by_name(resolved.catalog.as_ref())
            .context(FindCatalog {
                name: resolved.catalog,
            })? {
            Some(c) => c,
            None => return Ok(None),
        };

        let schema = match catalog
            .schema_by_name(resolved.schema.as_ref())
            .context(FindSchema {
                name: resolved.schema.to_string(),
            })? {
            Some(s) => s,
            None => {
                return SchemaNotFound {
                    name: resolved.schema.to_string(),
                }
                    .fail();
            }
        };

        schema
            .table_by_name(resolved.table.as_ref())
            .map_err(Box::new)
            .context(FindTable {
                name: resolved.table,
            })
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.function_registry.find_udf(name).context(FindUdf)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.function_registry.find_udaf(name).context(FindUdf)
    }

    // TODO: after supporting not only default catalog and schema, we should
    // refactor the tables collecting procedure.
    fn all_tables(&self) -> Result<Vec<TableRef>> {
        let catalog = self
            .manager
            .catalog_by_name(self.default_catalog)
            .with_context(|| FindCatalog {
                name: self.default_catalog,
            })?
            .with_context(|| CatalogNotFound {
                name: self.default_catalog,
            })?;

        let schema = catalog
            .schema_by_name(self.default_schema)
            .context(FindSchema {
                name: self.default_schema,
            })?
            .with_context(|| SchemaNotFound {
                name: self.default_schema,
            })?;

        schema.all_tables().with_context(|| GetAllTables {
            catalog_name: self.default_catalog,
            schema_name: self.default_schema,
        })
    }
}

/// 实现了metaProvider 和 contextProvider 其中前者的功能是通过代理实现的 fenquen
/// 它不 thread safe 
pub struct MetaAndContextProvider<'a, MetaProviderType> {
    /// Local cache for TableProvider to avoid create multiple adapter for the
    /// same table, also save all the table needed during planning
    tableContainer: RefCell<TableContainer>,
    /// Store the first error MetaProvider returns
    err: RefCell<Option<Error>>,
    metaProvider: &'a MetaProviderType,
    /// Read config for each table.
    config: ConfigOptions,
}

impl<'a, P: MetaProvider> MetaAndContextProvider<'a, P> {
    /// Create a adapter from meta provider
    pub fn new(meta_provider: &'a P, read_parallelism: usize) -> Self {
        let default_catalog = meta_provider.default_catalog_name().to_string();
        let default_schema = meta_provider.default_schema_name().to_string();
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = read_parallelism;

        Self {
            tableContainer: RefCell::new(TableContainer::new(default_catalog, default_schema)),
            err: RefCell::new(None),
            metaProvider: meta_provider,
            config,
        }
    }

    /// Consumes the adapter, returning the tables used during planning if no
    /// error occurs, otherwise returning the error
    pub fn try_into_container(self) -> Result<TableContainer> {
        if let Some(e) = self.err.into_inner() {
            return Err(e);
        }

        Ok(self.tableContainer.into_inner())
    }

    /// Save error if there is no existing error.
    ///
    /// The datafusion's ContextProvider can't return error, so here we save the
    /// error in the adapter and return None, also let datafusion
    /// return a provider not found error and abort the planning
    /// procedure.
    fn maybe_set_err(&self, err: Error) {
        if self.err.borrow().is_none() {
            *self.err.borrow_mut() = Some(err);
        }
    }

    pub fn table_source(&self, table_ref: TableRef) -> Arc<(dyn TableSource + 'static)> {
        let table_adapter = Arc::new(TableProviderImpl::new(table_ref));

        Arc::new(DefaultTableSource {
            table_provider: table_adapter,
        })
    }
}

impl<'a, P: MetaProvider> MetaProvider for MetaAndContextProvider<'a, P> {
    fn default_catalog_name(&self) -> &str {
        self.metaProvider.default_catalog_name()
    }

    fn default_schema_name(&self) -> &str {
        self.metaProvider.default_schema_name()
    }

    fn table(&self, name: TableReference) -> Result<Option<TableRef>> {
        self.metaProvider.table(name)
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.metaProvider.scalar_udf(name)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.metaProvider.aggregate_udf(name)
    }

    fn all_tables(&self) -> Result<Vec<TableRef>> {
        self.metaProvider.all_tables()
    }
}

impl<'a, P: MetaProvider> ContextProvider for MetaAndContextProvider<'a, P> {
    fn get_table_provider(&self, name: TableReference) -> std::result::Result<Arc<(dyn TableSource + 'static)>, DataFusionError> {
        // find in local cache
        if let Some(table_ref) = self.tableContainer.borrow().get(name.clone()) {
            return Ok(self.table_source(table_ref));
        }

        // find in meta provider
        // TODO: possible to remove this clone?
        match self.metaProvider.table(name.clone()) {
            Ok(Some(table)) => {
                self.tableContainer.borrow_mut().insert(name, table.clone());
                Ok(self.table_source(table))
            }
            Ok(None) => Err(DataFusionError::Execution(format!("table is not found, {:?}", format_table_reference(name)))),
            Err(e) => {
                let err_msg = format!("fail to find table, {:?}, err:{}", format_table_reference(name), e);
                self.maybe_set_err(e);
                Err(DataFusionError::Execution(err_msg))
            }
        }
    }

    // ScalarUDF is not supported now
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        // We don't cache udf used by the query because now we will register all udf to
        // datafusion's context.
        match self.metaProvider.scalar_udf(name) {
            Ok(Some(udf)) => Some(udf.to_datafusion_udf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    // AggregateUDF is not supported now
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match self.metaProvider.aggregate_udf(name) {
            Ok(Some(udaf)) => Some(udaf.to_datafusion_udaf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }

    // TODO: Variable Type is not supported now
    fn get_variable_type(
        &self,
        _variable_names: &[String],
    ) -> Option<common_types::schema::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config
    }
}

struct SchemaProviderImpl {
    catalogName: String,
    schemaName: String,
    tableContainer: Arc<TableContainer>,
}

#[async_trait]
impl SchemaProvider for SchemaProviderImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let _ = self.tableContainer.visit::<_, ()>(|name, table| {
            if name.catalog == self.catalogName && name.schema == self.schemaName {
                names.push(table.name().to_string());
            }
            Ok(())
        });
        names
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let name_ref = TableReference::Full {
            catalog: Cow::from(&self.catalogName),
            schema: Cow::from(&self.schemaName),
            table: Cow::from(name),
        };

        self.tableContainer.get(name_ref).map(|table_ref| Arc::new(TableProviderImpl::new(table_ref)) as _)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tableContainer.get(TableReference::parse_str(name)).is_some()
    }
}

#[derive(Default)]
pub struct CatalogProviderImpl {
    schemaName_schemaProvider: HashMap<String, Arc<SchemaProviderImpl>>,
}

impl CatalogProviderImpl {
    pub fn new_adapters(tableContainer: Arc<TableContainer>) -> HashMap<String, CatalogProviderImpl> {
        let mut catalogName_catalogProvider = HashMap::with_capacity(tableContainer.num_catalogs());

        let _ = tableContainer.visit::<_, ()>(|resolvedTableReference, _| {
            // get or create catalog
            let catalogProvider = match catalogName_catalogProvider.get_mut(resolvedTableReference.catalog.as_ref()) {
                Some(v) => v,
                None => catalogName_catalogProvider.entry(resolvedTableReference.catalog.to_string()).or_insert_with(CatalogProviderImpl::default),
            };

            // get or create schema
            if catalogProvider.schemaName_schemaProvider.get(resolvedTableReference.schema.as_ref()).is_none() {
                catalogProvider.schemaName_schemaProvider.insert(
                    resolvedTableReference.schema.to_string(),
                    Arc::new(SchemaProviderImpl {
                        catalogName: resolvedTableReference.catalog.to_string(),
                        schemaName: resolvedTableReference.schema.to_string(),
                        tableContainer: tableContainer.clone(),
                    }),
                );
            }

            Ok(())
        });

        catalogName_catalogProvider
    }
}

impl CatalogProvider for CatalogProviderImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemaName_schemaProvider.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemaName_schemaProvider.get(name).cloned().map(|v| v as Arc<dyn SchemaProvider>)
    }
}

/// Provide the description string for [`TableReference`] which hasn't derive [`Debug`] or implement [`std::fmt::Display`].
fn format_table_reference(table_ref: TableReference) -> String {
    match table_ref {
        TableReference::Bare { table } => format!("table:{table}"),
        TableReference::Partial { schema, table } => format!("schema:{schema}, table:{table}"),
        TableReference::Full { catalog, schema, table } => format!("catalog:{catalog}, schema:{schema}, table:{table}"),
    }
}
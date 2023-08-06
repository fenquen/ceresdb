// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table based catalog implementation

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use catalog::{
    self, consts,
    manager::{self, CatalogManager},
    schema::{
        self, AllocateTableId, CatalogMismatch, CreateExistTable, CreateOptions,
        CreateTableRequest, CreateTableWithCause, DropOptions, DropTableRequest,
        DropTableWithCause, NameRef, Schema, SchemaMismatch, SchemaRef, TooManyTable,
        WriteTableMeta,
    },
    Catalog, CatalogRef,
};
use generic_error::BoxError;
use log::{debug, info};
use macros::define_result;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use system_catalog::sys_catalog_table::{
    self, CreateCatalogRequest, CreateSchemaRequest, SysCatalogTable, VisitOptions,
    VisitOptionsBuilder, VisitorCatalogNotFound, VisitorInner, VisitorSchemaNotFound,
};
use table_engine::{
    engine::{TableEngineRef, TableState},
    table::{
        ReadOptions, SchemaId, SchemaIdGenerator, TableId, TableInfo, TableRef, TableSeq,
        TableSeqGenerator,
    },
};
use tokio::sync::Mutex;
use table_engine::engine::TableEngine;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build sys catalog table, err:{}", source))]
    BuildSysCatalog {
        source: sys_catalog_table::Error,
    },

    #[snafu(display("Failed to visit sys catalog table, err:{}", source))]
    VisitSysCatalog {
        source: sys_catalog_table::Error,
    },

    #[snafu(display("Failed to find table to update, name:{}.\nBacktrace:\n{}", name, backtrace))]
    UpdateTableNotFound { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to create catalog, catalog:{}, err:{}", catalog, source))]
    CreateCatalog {
        catalog: String,
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display("Failed to create schema, catalog:{}, schema:{}, err:{}", catalog, schema, source))]
    CreateSchema {
        catalog: String,
        schema: String,
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display("Invalid schema id and table seq, schema_id:{:?}, table_seq:{:?}.\nBacktrace:\n{}", schema_id, table_seq, backtrace, ))]
    InvalidSchemaIdAndTableSeq {
        schema_id: SchemaId,
        table_seq: TableSeq,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Table based catalog manager
pub struct CatalogManagerTableBased {
    /// Sys catalog table
    sysCatalogTable: Arc<SysCatalogTable>,
    catalogs: HashMap<String, Arc<CatalogTableBased>>,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<SchemaIdGenerator>,
}

impl CatalogManager for CatalogManagerTableBased {
    fn default_catalog_name(&self) -> NameRef {
        consts::DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> NameRef {
        consts::DEFAULT_SCHEMA
    }

    fn catalog_by_name(&self, name: NameRef) -> manager::Result<Option<CatalogRef>> {
        let catalog = self.catalogs.get(name).cloned().map(|v| v as _);
        Ok(catalog)
    }

    fn all_catalogs(&self) -> manager::Result<Vec<CatalogRef>> {
        Ok(self.catalogs.values().map(|v| v.clone() as _).collect())
    }
}

impl CatalogManagerTableBased {
    // TODO(yingwen): Define all constants in catalog crate.
    pub async fn new(tableEngine: Arc<dyn TableEngine>) -> Result<Self> {
        // create or open system.public.sys_catalog, will also create a space (catalog + schema) for system catalog.
        let sysCatalogTable = SysCatalogTable::new(tableEngine).await.context(BuildSysCatalog)?;

        let mut manager = Self {
            sysCatalogTable: Arc::new(sysCatalogTable),
            catalogs: HashMap::new(),
            schema_id_generator: Arc::new(SchemaIdGenerator::default()),
        };

        manager.init().await?;

        Ok(manager)
    }

    pub async fn fetch_table_infos(&mut self) -> Result<Vec<TableInfo>> {
        let catalog_table = self.sysCatalogTable.clone();

        let mut tableInfos = Vec::default();

        let vistorInnerImpl = VisitorInnerImpl {
            catalog_table: catalog_table.clone(),
            catalogs: &mut self.catalogs,
            schema_id_generator: self.schema_id_generator.clone(),
            table_infos: &mut tableInfos,
        };

        let visitOptions = VisitOptionsBuilder::default().visit_table().build();

        Self::visit_catalog_table_with_options(catalog_table,
                                               vistorInnerImpl,
                                               visitOptions).await?;

        Ok(tableInfos)
    }

    /// Load all data from sys catalog table.
    async fn init(&mut self) -> Result<()> {
        // The system catalog and schema in it is not persisted, so we add it manually.
        self.load_system_catalog();

        // Load all existent catalog/schema from catalog_table
        let catalog_table = self.sysCatalogTable.clone();

        let visitor_inner = VisitorInnerImpl {
            catalog_table: self.sysCatalogTable.clone(),
            catalogs: &mut self.catalogs,
            schema_id_generator: self.schema_id_generator.clone(),
            table_infos: &mut Vec::default(),
        };

        let visit_opts = VisitOptionsBuilder::default()
            .visit_catalog()
            .visit_schema()
            .build();

        Self::visit_catalog_table_with_options(catalog_table, visitor_inner, visit_opts).await?;

        // Create default catalog if it is not exists.
        self.maybe_create_default_catalog().await?;

        Ok(())
    }

    async fn visit_catalog_table_with_options(
        catalog_table: Arc<SysCatalogTable>,
        mut visitor_inner: VisitorInnerImpl<'_>,
        visit_opts: VisitOptions,
    ) -> Result<()> {
        let opts = ReadOptions::default();

        catalog_table
            .visit(opts, &mut visitor_inner, visit_opts)
            .await
            .context(VisitSysCatalog)
    }

    fn load_system_catalog(&mut self) {
        // Get the `sys_catalog` table and add it to tables.
        let table = self.sysCatalogTable.inner_table();
        let mut tables = SchemaTables::default();
        tables.insert(self.sysCatalogTable.table_id(), table);

        // Use schema id of schema `system/public` as last schema id.
        let schema_id = system_catalog::SYSTEM_SCHEMA_ID;
        self.schema_id_generator.set_last_schema_id(schema_id);

        // Create the default schema in system catalog.
        let schema = Arc::new(SchemaTableBased {
            belongingCatalogName: consts::SYSTEM_CATALOG.to_string(),
            name: consts::SYSTEM_CATALOG_SCHEMA.to_string(),
            id: schema_id,
            tables: RwLock::new(tables),
            mutex: Mutex::new(()),
            catalog_table: self.sysCatalogTable.clone(),
            table_seq_generator: TableSeqGenerator::default(),
        });
        // Use table seq of `sys_catalog` table as last table seq.
        schema
            .table_seq_generator
            .set_last_table_seq(system_catalog::MAX_SYSTEM_TABLE_SEQ);

        let mut schemas = HashMap::new();
        schemas.insert(schema.name().to_string(), schema);

        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.sysCatalogTable.clone();
        // Create the system catalog.
        let catalog = Arc::new(CatalogTableBased {
            name: consts::SYSTEM_CATALOG.to_string(),
            schemas: RwLock::new(schemas),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        });

        self.catalogs.insert(catalog.name().to_string(), catalog);
    }

    async fn maybe_create_default_catalog(&mut self) -> Result<()> {
        // Try to get default catalog, create it if not exists.
        let catalog = match self.catalogs.get(consts::DEFAULT_CATALOG) {
            Some(v) => v.clone(),
            None => {
                // Only system catalog should exists.
                assert_eq!(1, self.catalogs.len());

                // Default catalog is not exists, create and store it.
                self.create_catalog(CreateCatalogRequest {
                    catalog_name: consts::DEFAULT_CATALOG.to_string(),
                })
                    .await?
            }
        };

        // Create default schema if not exists.
        if catalog.find_schema(consts::DEFAULT_SCHEMA).is_none() {
            // Allocate schema id.
            let schema_id = self
                .schema_id_generator
                .alloc_schema_id()
                .expect("Schema id of default catalog should be valid");

            self.add_schema_to_catalog(
                CreateSchemaRequest {
                    catalog_name: consts::DEFAULT_CATALOG.to_string(),
                    schema_name: consts::DEFAULT_SCHEMA.to_string(),
                    schema_id,
                },
                &catalog,
            )
                .await?;
        }

        Ok(())
    }

    async fn create_catalog(&mut self, request: CreateCatalogRequest) -> Result<Arc<CatalogTableBased>> {
        let catalog_name = request.catalog_name.clone();

        self.sysCatalogTable
            .create_catalog(request)
            .await
            .context(CreateCatalog {
                catalog: &catalog_name,
            })?;

        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.sysCatalogTable.clone();
        let catalog = Arc::new(CatalogTableBased {
            name: catalog_name.clone(),
            schemas: RwLock::new(HashMap::new()),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        });

        self.catalogs.insert(catalog_name, catalog.clone());

        Ok(catalog)
    }

    async fn add_schema_to_catalog(
        &mut self,
        request: CreateSchemaRequest,
        catalog: &CatalogTableBased,
    ) -> Result<Arc<SchemaTableBased>> {
        let schema_name = request.schema_name.clone();
        let schema_id = request.schema_id;

        self.sysCatalogTable
            .create_schema(request)
            .await
            .context(CreateSchema {
                catalog: &catalog.name,
                schema: &schema_name,
            })?;

        let schema = Arc::new(SchemaTableBased::new(
            &catalog.name,
            &schema_name,
            schema_id,
            self.sysCatalogTable.clone(),
        ));

        catalog.insert_schema_into_memory(schema.clone());

        Ok(schema)
    }
}

type CatalogMap = HashMap<String, Arc<CatalogTableBased>>;

/// Sys catalog visitor implementation, used to load catalog info
struct VisitorInnerImpl<'a> {
    catalog_table: Arc<SysCatalogTable>,
    catalogs: &'a mut CatalogMap,
    schema_id_generator: Arc<SchemaIdGenerator>,
    table_infos: &'a mut Vec<TableInfo>,
}

#[async_trait]
impl<'a> VisitorInner for VisitorInnerImpl<'a> {
    fn visit_catalog(&mut self, request: CreateCatalogRequest) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit catalog, request:{:?}", request);
        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.catalog_table.clone();

        let catalog = CatalogTableBased {
            name: request.catalog_name.to_string(),
            schemas: RwLock::new(HashMap::new()),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        };

        // Register catalog.
        self.catalogs
            .insert(request.catalog_name, Arc::new(catalog));

        Ok(())
    }

    fn visit_schema(&mut self, request: CreateSchemaRequest) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit schema, request:{:?}", request);

        let catalog =
            self.catalogs
                .get_mut(&request.catalog_name)
                .context(VisitorCatalogNotFound {
                    catalog: &request.catalog_name,
                })?;

        let schema_id = request.schema_id;
        let schema = Arc::new(SchemaTableBased::new(
            &request.catalog_name,
            &request.schema_name,
            schema_id,
            self.catalog_table.clone(),
        ));

        // If schema exists, we overwrite it.
        catalog.insert_schema_into_memory(schema);

        // Update last schema id.
        if self.schema_id_generator.last_schema_id_u32() < schema_id.as_u32() {
            self.schema_id_generator.set_last_schema_id(schema_id);
        }

        Ok(())
    }

    fn visit_tables(&mut self, table_info: TableInfo) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit tables, table_info:{:?}", table_info);

        let catalog =
            self.catalogs
                .get_mut(&table_info.catalog_name)
                .context(VisitorCatalogNotFound {
                    catalog: &table_info.catalog_name,
                })?;
        let schema =
            catalog
                .find_schema(&table_info.schema_name)
                .context(VisitorSchemaNotFound {
                    catalog: &table_info.catalog_name,
                    schema: &table_info.schema_name,
                })?;

        // Update max table sequence of the schema.
        let table_id = table_info.table_id;
        let table_seq = TableSeq::from(table_id);
        if table_seq.as_u64() >= schema.table_seq_generator.last_table_seq_u64() {
            schema.table_seq_generator.set_last_table_seq(table_seq);
        }

        // Only the stable/altering table can be opened.
        if !matches!(table_info.state, TableState::Stable) {
            debug!(
                "Visitor visit a unstable table, table_info:{:?}",
                table_info
            );
            return Ok(());
        }

        // Collect table infos for later opening.
        self.table_infos.push(table_info);

        Ok(())
    }
}

type SchemaMap = HashMap<String, Arc<SchemaTableBased>>;

/// Table based catalog
struct CatalogTableBased {
    /// Catalog name
    name: String,
    /// Schemas of catalog
    // Now the Schema trait does not support create schema, so we use impl type here
    schemas: RwLock<HashMap<String, Arc<SchemaTableBased>>>,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<SchemaIdGenerator>,
    /// Sys catalog table
    catalog_table: Arc<SysCatalogTable>,
    /// Mutex
    ///
    /// Protects:
    /// - create schema
    /// - persist to default catalog
    mutex: Mutex<()>,
}

impl CatalogTableBased {
    /// Insert schema
    fn insert_schema_into_memory(&self, schema: Arc<SchemaTableBased>) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(schema.name().to_string(), schema);
    }

    fn find_schema(&self, schema_name: &str) -> Option<Arc<SchemaTableBased>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(schema_name).cloned()
    }
}

// TODO(yingwen): Support add schema (with options to control schema persistence)
#[async_trait]
impl Catalog for CatalogTableBased {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn schema_by_name(&self, name: NameRef) -> catalog::Result<Option<SchemaRef>> {
        let schemas = self.schemas.read().unwrap();
        let schema = schemas.get(name).cloned().map(|v| v as _);
        Ok(schema)
    }

    async fn create_schema<'a>(&'a self, name: NameRef<'a>) -> catalog::Result<()> {
        // Check schema existence
        if self.schema_by_name(name)?.is_some() {
            return Ok(());
        }

        // Lock schema and persist schema to default catalog
        let _lock = self.mutex.lock().await;
        // Check again
        if self.schema_by_name(name)?.is_some() {
            return Ok(());
        }

        // Allocate schema id.
        let schema_id = self
            .schema_id_generator
            .alloc_schema_id()
            .expect("Schema id of default catalog should be valid");

        let request = CreateSchemaRequest {
            catalog_name: self.name.to_string(),
            schema_name: name.to_string(),
            schema_id,
        };

        let schema_id = request.schema_id;

        self.catalog_table
            .create_schema(request)
            .await
            .box_err()
            .context(catalog::CreateSchemaWithCause {
                catalog: &self.name,
                schema: &name.to_string(),
            })?;

        let schema = Arc::new(SchemaTableBased::new(
            &self.name,
            name,
            schema_id,
            self.catalog_table.clone(),
        ));

        self.insert_schema_into_memory(schema);
        info!(
            "create schema success, catalog:{}, schema:{}",
            &self.name, name
        );
        Ok(())
    }

    fn all_schemas(&self) -> catalog::Result<Vec<SchemaRef>> {
        Ok(self
            .schemas
            .read()
            .unwrap()
            .iter()
            .map(|(_, v)| v.clone() as _)
            .collect())
    }
}

/// Table based schema
struct SchemaTableBased {
    belongingCatalogName: String,
    name: String,
    id: SchemaId,
    tables: RwLock<SchemaTables>,
    /// Mutex
    ///
    /// Protects:
    /// - add/drop/alter table
    /// - persist to sys catalog table
    mutex: Mutex<()>,
    /// Sys catalog table
    catalog_table: Arc<SysCatalogTable>,
    table_seq_generator: TableSeqGenerator,
}

impl SchemaTableBased {
    fn new(
        catalog_name: &str,
        schema_name: &str,
        schema_id: SchemaId,
        catalog_table: Arc<SysCatalogTable>,
    ) -> Self {
        Self {
            belongingCatalogName: catalog_name.to_string(),
            name: schema_name.to_string(),
            id: schema_id,
            tables: RwLock::new(SchemaTables::default()),
            mutex: Mutex::new(()),
            catalog_table,
            table_seq_generator: TableSeqGenerator::default(),
        }
    }

    fn validate_schema_info(&self, catalog_name: &str, schema_name: &str) -> schema::Result<()> {
        ensure!(
            self.belongingCatalogName == catalog_name,
            CatalogMismatch {
                expect: &self.belongingCatalogName,
                given: catalog_name,
            }
        );
        ensure!(
            self.name == schema_name,
            SchemaMismatch {
                expect: &self.name,
                given: schema_name,
            }
        );

        Ok(())
    }

    /// Insert table into memory, wont check existence
    fn insert_table_into_memory(&self, table_id: TableId, table: TableRef) {
        let mut tables = self.tables.write().unwrap();
        tables.insert(table_id, table);
    }

    /// Remove table in memory, wont check existence
    fn remove_table_in_memory(&self, table_name: &str) {
        let mut tables = self.tables.write().unwrap();
        tables.remove(table_name);
    }

    /// Check table existence in read lock
    ///
    /// If table exists:
    /// - if create_if_not_exists is true, return Ok
    /// - if create_if_not_exists is false, return Error
    fn check_create_table_read(
        &self,
        table_name: &str,
        create_if_not_exists: bool,
    ) -> schema::Result<Option<TableRef>> {
        let tables = self.tables.read().unwrap();
        if let Some(table) = tables.tables_by_name.get(table_name) {
            // Already exists
            if create_if_not_exists {
                // Create if not exists is set
                return Ok(Some(table.clone()));
            }
            // Create if not exists is not set, need to return error
            return CreateExistTable { table: table_name }.fail();
        }

        Ok(None)
    }

    fn find_table_by_name(&self, name: NameRef) -> Option<TableRef> {
        self.tables
            .read()
            .unwrap()
            .tables_by_name
            .get(name)
            .cloned()
    }

    async fn alloc_table_id<'a>(&self, name: NameRef<'a>) -> schema::Result<TableId> {
        let table_seq = self
            .table_seq_generator
            .alloc_table_seq()
            .context(TooManyTable {
                schema: &self.name,
                table: name,
            })?;

        TableId::with_seq(self.id, table_seq)
            .context(InvalidSchemaIdAndTableSeq {
                schema_id: self.id,
                table_seq,
            })
            .box_err()
            .context(AllocateTableId {
                schema: &self.name,
                table: name,
            })
    }
}

#[derive(Default)]
struct SchemaTables {
    tables_by_name: HashMap<String, TableRef>,
    tables_by_id: HashMap<TableId, TableRef>,
}

impl SchemaTables {
    fn insert(&mut self, table_id: TableId, table: TableRef) {
        self.tables_by_name
            .insert(table.name().to_string(), table.clone());
        self.tables_by_id.insert(table_id, table);
    }

    fn remove(&mut self, name: NameRef) {
        if let Some(table) = self.tables_by_name.remove(name) {
            self.tables_by_id.remove(&table.id());
        }
    }
}

#[async_trait]
impl Schema for SchemaTableBased {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn id(&self) -> SchemaId {
        self.id
    }

    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let table = self
            .tables
            .read()
            .unwrap()
            .tables_by_name
            .get(name)
            .cloned();
        Ok(table)
    }

    // TODO(yingwen): Do not persist if engine is memory engine.
    async fn create_table(&self,
                          request: CreateTableRequest,
                          opts: CreateOptions) -> schema::Result<TableRef> {
        info!("Table based catalog manager create table, request:{:?}",request);

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        // TODO(yingwen): Validate table id is unique.

        // Check table existence
        if let Some(table) =
            self.check_create_table_read(&request.table_name, opts.create_if_not_exists)?
        {
            return Ok(table);
        }

        // Lock schema and persist table to sys catalog table
        let _lock = self.mutex.lock().await;
        // Check again
        if let Some(table) =
            self.check_create_table_read(&request.table_name, opts.create_if_not_exists)?
        {
            return Ok(table);
        }

        // Create table
        let table_id = self.alloc_table_id(&request.table_name).await?;
        let request = request.into_engine_create_request(Some(table_id), self.id);
        let table_name = request.table_name.clone();
        let table = opts
            .table_engine
            .create_table(request.clone())
            .await
            .box_err()
            .context(CreateTableWithCause)?;
        assert_eq!(table_name, table.name());

        self.catalog_table
            .create_table(request.clone().into())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        {
            // Insert into memory
            let mut tables = self.tables.write().unwrap();
            tables.insert(request.table_id, table.clone());
        }

        Ok(table)
    }

    async fn drop_table(
        &self,
        mut request: DropTableRequest,
        opts: DropOptions,
    ) -> schema::Result<bool> {
        info!(
            "Table based catalog manager drop table, request:{:?}",
            request
        );

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        if self.find_table_by_name(&request.table_name).is_none() {
            return Ok(false);
        };

        let _lock = self.mutex.lock().await;
        // double check whether the table to drop exists.
        let table = match self.find_table_by_name(&request.table_name) {
            Some(v) => v,
            None => return Ok(false),
        };

        // Determine the real engine type of the table to drop.
        // FIXME(xikai): the engine should not be part of the DropRequest.
        request.engine = table.engine_type().to_string();
        let request = request.into_engine_drop_request(self.id);

        // Prepare to drop table info in the sys_catalog.
        self.catalog_table
            .prepare_drop_table(request.clone())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        let dropped = opts
            .table_engine
            .drop_table(request.clone())
            .await
            .box_err()
            .context(DropTableWithCause)?;

        info!(
            "Table engine drop table successfully, request:{:?}, dropped:{}",
            request, dropped
        );

        // Update the drop table record into the sys_catalog_table.
        self.catalog_table
            .drop_table(request.clone())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        {
            let mut tables = self.tables.write().unwrap();
            tables.remove(&request.table_name);
        };

        info!(
            "Table based catalog manager drop table successfully, request:{:?}",
            request
        );

        return Ok(true);
    }

    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        Ok(self
            .tables
            .read()
            .unwrap()
            .tables_by_name
            .values()
            .cloned()
            .collect())
    }

    fn register_table(&self, table: TableRef) {
        self.insert_table_into_memory(table.id(), table);
    }

    fn unregister_table(&self, table_name: &str) {
        self.remove_table_in_memory(table_name);
    }
}
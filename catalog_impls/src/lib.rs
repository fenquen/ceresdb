// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use catalog::{
    consts::SYSTEM_CATALOG,
    manager::{CatalogManager},
    schema::NameRef,
    CatalogRef,
};
use system_catalog::{tables::Tables, SystemTableAdapter};

use crate::system_tables::{SystemTables, SystemTablesBuilder};

mod system_tables;
pub mod table_based;
pub mod volatile;

/// CatalogManagerImpl is a wrapper for system and user tables
#[derive(Clone)]
pub struct CatalogManagerDelegate {
    system_tables: SystemTables,
    user_catalog_manager: Arc<dyn CatalogManager>,
}

impl CatalogManagerDelegate {
    pub fn new(manager: Arc<dyn CatalogManager>) -> Self {
        let mut system_tables_builder = SystemTablesBuilder::new();
        system_tables_builder = system_tables_builder.insert_table(SystemTableAdapter::new(Tables::new(manager.clone())));
        Self {
            system_tables: system_tables_builder.build(),
            user_catalog_manager: manager,
        }
    }
}

impl CatalogManager for CatalogManagerDelegate {
    fn default_catalog_name(&self) -> NameRef {
        self.user_catalog_manager.default_catalog_name()
    }

    fn default_schema_name(&self) -> NameRef {
        self.user_catalog_manager.default_schema_name()
    }

    fn catalog_by_name(&self, name: NameRef) -> catalog::manager::Result<Option<CatalogRef>> {
        match name {
            SYSTEM_CATALOG => Ok(Some(Arc::new(self.system_tables.clone()))),
            _ => self.user_catalog_manager.catalog_by_name(name),
        }
    }

    fn all_catalogs(&self) -> catalog::manager::Result<Vec<CatalogRef>> {
        self.user_catalog_manager.all_catalogs()
    }
}

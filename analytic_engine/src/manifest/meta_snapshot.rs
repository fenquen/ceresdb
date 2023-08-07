// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Meta data of manifest.

use log::debug;
use macros::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    manifest::meta_edit::{AddTableMeta, MetaUpdate},
    table::version::TableVersionMeta,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Apply update on non-exist table, meta update:{:?}\nBacktrace:\n{}", metaUpdate, backtrace))]
    TableNotFound {
        metaUpdate: MetaUpdate,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[derive(Debug, Clone, PartialEq)]
pub struct MetaSnapshot {
    pub table_meta: AddTableMeta,
    pub version_meta: Option<TableVersionMeta>,
}

#[derive(Clone, Debug, Default)]
pub struct MetaSnapshotBuilder {
    addTableMeta: Option<AddTableMeta>,
    tableVersionMeta: Option<TableVersionMeta>,
}

impl MetaSnapshotBuilder {
    pub fn new(table_meta: Option<AddTableMeta>, version_meta: Option<TableVersionMeta>) -> Self {
        Self {
            addTableMeta: table_meta,
            tableVersionMeta: version_meta,
        }
    }

    pub fn build(mut self) -> Option<MetaSnapshot> {
        let version_meta = self.tableVersionMeta.take();
        self.addTableMeta.map(|v| MetaSnapshot {
            table_meta: v,
            version_meta,
        })
    }

    #[inline]
    pub fn is_table_exists(&self) -> bool {
        self.addTableMeta.is_some()
    }

    /// Apply the meta update.
    ///
    /// Any update except [`MetaUpdate::AddTable`] on a non-exist table will fail.
    pub fn apply_update(&mut self, metaUpdate: MetaUpdate) -> Result<()> {
        debug!("Apply meta update, update:{:?}", metaUpdate);

        if let MetaUpdate::AddTable(_) = &metaUpdate {
        } else {
            ensure!(self.is_table_exists(), TableNotFound { metaUpdate });
        }

        match metaUpdate {
            MetaUpdate::AddTable(meta) => {
                self.addTableMeta = Some(meta);
            }
            MetaUpdate::VersionEdit(meta) => {
                let edit = meta.into_version_edit();
                let mut version = self.tableVersionMeta.take().unwrap_or_default();
                version.apply_edit(edit);
                self.tableVersionMeta = Some(version);
            }
            MetaUpdate::AlterSchema(meta) => {
                let table_meta = self.addTableMeta.as_mut().unwrap();
                table_meta.schema = meta.schema;
            }
            MetaUpdate::AlterOptions(meta) => {
                let table_meta = self.addTableMeta.as_mut().unwrap();
                table_meta.opts = meta.options;
            }
            MetaUpdate::DropTable(meta) => {
                self.addTableMeta = None;
                self.tableVersionMeta = None;
                debug!("Apply drop table meta update, removed table:{}",meta.table_name,);
            }
        }

        Ok(())
    }
}

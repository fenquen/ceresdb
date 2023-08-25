// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Context for instance

use std::{fmt, sync::Arc};

use table_engine::engine::EngineRuntimes;

use crate::{sst::meta_data::cache::MetaCacheRef, AnalyticEngineConfig};

/// Context for instance open
pub struct OpenedTableEngineInstanceContext {
    /// Engine config
    pub analyticEngineConfig: AnalyticEngineConfig,

    /// Background job runtime
    pub engineRuntimes: Arc<EngineRuntimes>,

    /// Sst meta data cache.
    pub meta_cache: Option<MetaCacheRef>,
}

impl fmt::Debug for OpenedTableEngineInstanceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenContext").field("config", &self.analyticEngineConfig).finish()
    }
}

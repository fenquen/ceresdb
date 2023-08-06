// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Instance contains shared states of service

use std::sync::Arc;

use catalog::manager::ManagerRef;
use df_operator::registry::FunctionRegistryRef;
use interpreters::table_manipulator::TableManipulatorRef;
use table_engine::{engine::TableEngineRef, remote::RemoteEngineRef};

use crate::limiter::Limiter;

/// A cluster instance. Usually there is only one instance per cluster
pub struct Instance<QueryExecutor> {
    pub catalog_manager: ManagerRef,
    pub query_executor: QueryExecutor,
    pub table_engine: TableEngineRef,
    pub partition_table_engine: TableEngineRef,
    // User defined functions registry.
    pub function_registry: FunctionRegistryRef,
    pub limiter: Limiter,
    pub table_manipulator: TableManipulatorRef,
    pub remote_engine_ref: RemoteEngineRef,
}

/// A reference counted instance pointer
pub type InstanceRef<Q> = Arc<Instance<Q>>;

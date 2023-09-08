// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.


use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{AnalyticTableEngineBuilder, KafkaWalsOpener, RocksDBWalsOpener, WalsOpener},
    WalStorageConfig,
};
use catalog::{manager::ManagerRef, schema::OpenOptions, table_operator::TableOperator};
use catalog_impls::{table_based::CatalogManagerTableBased, volatile, CatalogManagerDelegate};
use cluster::{cluster_impl::ClusterImpl, config::ClusterConfig, shard_set::ShardSet};
use df_operator::registry::FunctionRegistryImpl;
use interpreters::table_manipulator::meta_based;
use log::info;
use interpreters::table_manipulator::catalog_based::TableManipulatorCatalogBased;
use logger::RuntimeLevel;
use meta_client::{meta_impl, types::NodeMetaInfo};
use proxy::{
    limiter::Limiter,
    schema_config_provider::{
        cluster_based::ClusterBasedProvider, config_based::ConfigBasedProvider,
    },
};
use query_engine::executor::{QueryExecutor, QueryExecutorImpl};
use router::{rule_based::ClusterView, ClusterBasedRouter, RuleBasedRouter};
use server::{
    config::{StaticRouteConfig, StaticTopologyConfig},
    local_tables::LocalTablesRecoverer,
    server::Builder,
};
use table_engine::{engine::EngineRuntimes, memory::MemoryTableEngine, proxy::TableEngineProxy};
use tracing_util::{
    self,
    tracing_appender::{non_blocking::WorkerGuard, rolling::Rotation},
};

use crate::{
    config::{ClusterDeployment, Config, RuntimeConfig},
    signal_handler,
};

/// Setup log with given `config`, returns the runtime log level switch.
pub fn setup_logger(config: &Config) -> RuntimeLevel {
    logger::init_log(&config.logger).expect("Failed to init log.")
}

/// Setup tracing with given `config`, returns the writer guard.
pub fn setup_tracing(config: &Config) -> WorkerGuard {
    tracing_util::init_tracing_with_file(&config.tracing, &config.node.addr, Rotation::NEVER)
}

fn build_runtime(name: &str, threads_num: usize) -> runtime::Runtime {
    runtime::Builder::default()
        .worker_threads(threads_num)
        .thread_name(name)
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

fn build_engine_runtimes(config: &RuntimeConfig) -> EngineRuntimes {
    EngineRuntimes {
        read_runtime: Arc::new(build_runtime("ceres-read", config.read_thread_num)),
        write_runtime: Arc::new(build_runtime("ceres-write", config.write_thread_num)),
        compact_runtime: Arc::new(build_runtime("ceres-compact", config.compact_thread_num)),
        meta_runtime: Arc::new(build_runtime("ceres-meta", config.meta_thread_num)),
        default_runtime: Arc::new(build_runtime("ceres-default", config.default_thread_num)),
        io_runtime: Arc::new(build_runtime("ceres-io", config.io_thread_num)),
    }
}

/// Run a server, returns when the server is shutdown by user
pub fn run_server(config: Config, logLevel: RuntimeLevel) {
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();
    let logLevel = Arc::new(logLevel);

    info!("server start, config:{:#?}", config);

    runtimes.default_runtime.block_on(async {
        match config.analytic.walStorageConfig {
            WalStorageConfig::RocksDB(_) => run_server_with_runtimes::<RocksDBWalsOpener>(config, engine_runtimes, logLevel).await,
            WalStorageConfig::Kafka(_) => run_server_with_runtimes::<KafkaWalsOpener>(config, engine_runtimes, logLevel).await,
        }
    });
}

async fn run_server_with_runtimes<T>(config: Config,
                                     engine_runtimes: Arc<EngineRuntimes>,
                                     logLevel: Arc<RuntimeLevel>) where T: WalsOpener {
    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry.load_functions().expect("failed to create function registry");
    let function_registry = Arc::new(function_registry);

    // Create query executor
    let query_executor = QueryExecutorImpl::new(config.query_engine.clone());

    // Config limiter
    let limiter = Limiter::new(config.limiter.clone());
    let config_content = toml::to_string(&config).expect("Fail to serialize config");

    let serverBuilder =
        Builder::new(config.server.clone())
            .node_addr(config.node.addr.clone())
            .config_content(config_content)
            .engine_runtimes(engine_runtimes.clone())
            .log_runtime(logLevel.clone())
            .query_executor(query_executor)
            .function_registry(function_registry)
            .limiter(limiter);

    let walsOpener = T::default();
    let serverBuilder = match &config.cluster_deployment {
        None => build_without_meta(&config,
                                   &StaticRouteConfig::default(),
                                   serverBuilder,
                                   engine_runtimes.clone(),
                                   walsOpener).await,

        Some(ClusterDeployment::NoMeta(v)) =>
            build_without_meta(&config,
                               v,
                               serverBuilder,
                               engine_runtimes.clone(),
                               walsOpener).await,
        Some(ClusterDeployment::WithMeta(cluster_config)) =>
            build_with_meta(&config,
                            cluster_config,
                            serverBuilder,
                            engine_runtimes.clone(),
                            walsOpener).await,
    };

    // Build and start server
    let mut server = serverBuilder.build().expect("Failed to create server");
    server.start().await.expect("Failed to start server");

    // Wait for signal
    signal_handler::wait_for_signal();

    // Stop server
    server.stop().await;
}

async fn build_table_engine_proxy(analyticTableEngineBuilder: AnalyticTableEngineBuilder<'_>) -> Arc<TableEngineProxy> {
    Arc::new(TableEngineProxy {
        memoryTableEngine: MemoryTableEngine,
        analyticTableEngine: analyticTableEngineBuilder.build().await.expect("failed to setup analytic engine"),
    })
}

async fn build_with_meta<Q: QueryExecutor + 'static, T: WalsOpener>(config: &Config,
                                                                    cluster_config: &ClusterConfig,
                                                                    builder: Builder<Q>,
                                                                    runtimes: Arc<EngineRuntimes>,
                                                                    wal_opener: T) -> Builder<Q> {
    // build meta related modules.
    let node_meta_info = NodeMetaInfo {
        addr: config.node.addr.clone(),
        port: config.server.grpc_port,
        zone: config.node.zone.clone(),
        idc: config.node.idc.clone(),
        binary_version: config.node.binary_version.clone(),
    };

    info!("build ceresdb with node meta info:{node_meta_info:?}");

    let endpoint = node_meta_info.endpoint();
    let meta_client =
        meta_impl::build_meta_client(cluster_config.meta_client.clone(), node_meta_info)
            .await
            .expect("fail to build meta client");

    let shard_set = ShardSet::default();
    let cluster = {
        let cluster_impl = ClusterImpl::try_new(
            endpoint,
            shard_set.clone(),
            meta_client.clone(),
            cluster_config.clone(),
            runtimes.meta_runtime.clone(),
        )
            .await
            .unwrap();
        Arc::new(cluster_impl)
    };
    let router = Arc::new(ClusterBasedRouter::new(
        cluster.clone(),
        config.server.route_cache.clone(),
    ));

    let opened_wals = wal_opener
        .open_wals(&config.analytic.walStorageConfig, runtimes.clone())
        .await
        .expect("Failed to setup analytic engine");
    let engine_builder = AnalyticTableEngineBuilder {
        analyticConfig: &config.analytic,
        engine_runtimes: runtimes.clone(),
        opened_wals: opened_wals.clone(),
    };
    let engine_proxy = build_table_engine_proxy(engine_builder).await;

    let meta_based_manager_ref =
        Arc::new(volatile::CatalogManagerMemoryBased::new(shard_set, meta_client.clone()));

    // Build catalog manager.
    let catalog_manager = Arc::new(CatalogManagerDelegate::new(meta_based_manager_ref));

    let table_manipulator = Arc::new(meta_based::TableManipulatorMetaBased::new(meta_client));

    let schema_config_provider = Arc::new(ClusterBasedProvider::new(cluster.clone()));
    builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .cluster(cluster)
        .opened_wals(opened_wals)
        .router(router)
        .schema_config_provider(schema_config_provider)
}

async fn build_without_meta<Executor: QueryExecutor + 'static, T: WalsOpener>(config: &Config,
                                                                              static_route_config: &StaticRouteConfig,
                                                                              serverBuilder: Builder<Executor>,
                                                                              engineRuntimes: Arc<EngineRuntimes>,
                                                                              walsOpener: T) -> Builder<Executor> {
    let opened_wals =
        walsOpener.open_wals(&config.analytic.walStorageConfig,
                             engineRuntimes.clone()).await.expect("Failed to setup analytic engine");

    let analyticTableEngineBuilder = AnalyticTableEngineBuilder {
        analyticConfig: &config.analytic,
        engine_runtimes: engineRuntimes.clone(),
        opened_wals: opened_wals.clone(),
    };

    let tableEngineProxy = build_table_engine_proxy(analyticTableEngineBuilder).await;

    // Create catalog manager, use analytic engine as backend.
    let mut catalogManagerTableBased =
        CatalogManagerTableBased::new(tableEngineProxy.analyticTableEngine.clone()).await.expect("Failed to create catalog manager");

    // Get collected table infos
    let tableInfoVec = catalogManagerTableBased.getTableInfos().await.expect("Failed to fetch table infos for opening");

    let catalogManagerDelegate = Arc::new(CatalogManagerDelegate::new(Arc::new(catalogManagerTableBased)));
    let table_operator = TableOperator::new(catalogManagerDelegate.clone());
    let table_manipulator = Arc::new(TableManipulatorCatalogBased::new(table_operator.clone()));

    // Iterate the table infos to recover.
    let open_opts = OpenOptions {
        table_engine: tableEngineProxy.clone(),
    };

    // Create local tables recoverer.
    let local_tables_recoverer = LocalTablesRecoverer::new(tableInfoVec, table_operator, open_opts);

    // Create schema in default catalog.
    create_static_topology_schema(catalogManagerDelegate.clone(),
                                  static_route_config.topology.clone()).await;

    // Build static router and schema config provider
    let cluster_view = ClusterView::from(&static_route_config.topology);
    let schema_configs = cluster_view.schema_configs.clone();
    let router = Arc::new(RuleBasedRouter::new(cluster_view, static_route_config.rules.clone()));
    let schema_config_provider = Arc::new(ConfigBasedProvider::new(
        schema_configs, config.server.default_schema_config.clone(),
    ));

    serverBuilder
        .table_engine(tableEngineProxy)
        .catalog_manager(catalogManagerDelegate)
        .table_manipulator(table_manipulator)
        .router(router)
        .opened_wals(opened_wals)
        .schema_config_provider(schema_config_provider)
        .local_tables_recoverer(local_tables_recoverer)
}

async fn create_static_topology_schema(catalog_mgr: ManagerRef,
                                       static_topology_config: StaticTopologyConfig) {
    let default_catalog =
        catalog_mgr.catalog_by_name(catalog_mgr.default_catalog_name())
        .expect("fail to retrieve default catalog").expect("Default catalog doesn't exist");

    for schema_shard_view in static_topology_config.schema_shards {
        default_catalog.create_schema(&schema_shard_view.schema).await.unwrap_or_else(|_| panic!("Fail to create schema:{}", schema_shard_view.schema));
        info!("create static topology in default catalog:{}, schema:{}",catalog_mgr.default_catalog_name(),&schema_shard_view.schema);
    }
}

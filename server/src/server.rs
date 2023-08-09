// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server

use std::sync::Arc;

use analytic_engine::setup::OpenedWals;
use catalog::manager::ManagerRef;
use cluster::ClusterRef;
use df_operator::registry::FunctionRegistryRef;
use interpreters::table_manipulator::TableManipulatorRef;
use log::{info, warn};
use logger::RuntimeLevel;
use macros::define_result;
use partition_table_engine::PartitionTableEngine;
use proxy::{
    instance::{Instance, InstanceRef},
    limiter::Limiter,
    schema_config_provider::SchemaConfigProviderRef,
    Proxy,
};
use query_engine::executor::QueryExecutor as QueryExecutor;
use remote_engine_client::RemoteEngineImpl;
use router::{endpoint::Endpoint, RouterRef};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};

use crate::{
    config::ServerConfig,
    grpc::{self, RpcServices},
    http::{self, HttpConfig, Service},
    local_tables::{self, LocalTablesRecoverer},
    mysql,
    mysql::error::Error as MysqlError,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing engine runtimes.\nBacktrace:\n{}", backtrace))]
    MissingEngineRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing log runtime.\nBacktrace:\n{}", backtrace))]
    MissingLogRuntime { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Missing catalog manager.\nBacktrace:\n{}", backtrace))]
    MissingCatalogManager { backtrace: Backtrace },

    #[snafu(display("Missing query executor.\nBacktrace:\n{}", backtrace))]
    MissingQueryExecutor { backtrace: Backtrace },

    #[snafu(display("Missing table engine.\nBacktrace:\n{}", backtrace))]
    MissingTableEngine { backtrace: Backtrace },

    #[snafu(display("Missing table manipulator.\nBacktrace:\n{}", backtrace))]
    MissingTableManipulator { backtrace: Backtrace },

    #[snafu(display("Missing function registry.\nBacktrace:\n{}", backtrace))]
    MissingFunctionRegistry { backtrace: Backtrace },

    #[snafu(display("Missing wals.\nBacktrace:\n{}", backtrace))]
    MissingWals { backtrace: Backtrace },

    #[snafu(display("Missing limiter.\nBacktrace:\n{}", backtrace))]
    MissingLimiter { backtrace: Backtrace },

    #[snafu(display("Http service failed, msg:{}, err:{}", msg, source))]
    HttpService {
        msg: String,
        source: http::Error,
    },

    #[snafu(display("Failed to build mysql service, err:{}", source))]
    BuildMysqlService { source: MysqlError },

    #[snafu(display("Failed to start mysql service, err:{}", source))]
    StartMysqlService { source: MysqlError },

    #[snafu(display("Failed to register system catalog, err:{}", source))]
    RegisterSystemCatalog { source: catalog::manager::Error },

    #[snafu(display("Failed to build grpc service, err:{}", source))]
    BuildGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start grpc service, err:{}", source))]
    StartGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start cluster, err:{}", source))]
    StartCluster { source: cluster::Error },

    #[snafu(display("Failed to open tables in standalone mode, err:{}", source))]
    OpenLocalTables { source: local_tables::Error },
}

define_result!(Error);

// TODO(yingwen): Consider a config manager
/// Server
pub struct Server<QueryExecutor0: QueryExecutor + 'static> {
    http_service: Service<QueryExecutor0>,
    rpc_services: RpcServices<QueryExecutor0>,
    mysql_service: mysql::MysqlService<QueryExecutor0>,
    instance: InstanceRef<QueryExecutor0>,
    cluster: Option<ClusterRef>,
    local_tables_recoverer: Option<LocalTablesRecoverer>,
}

impl<Q: QueryExecutor + 'static> Server<Q> {
    pub async fn stop(mut self) {
        self.rpc_services.shutdown().await;
        self.http_service.stop();
        self.mysql_service.shutdown();

        if let Some(cluster) = &self.cluster {
            cluster.stop().await.expect("fail to stop cluster");
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        // Run in standalone mode
        if let Some(local_tables_recoverer) = &self.local_tables_recoverer {
            info!("server start, open local tables");
            local_tables_recoverer.recover().await.context(OpenLocalTables)?;
        }

        // Run in cluster mode
        if let Some(cluster) = &self.cluster {
            info!("server start, start cluster");
            cluster.start().await.context(StartCluster)?;
        }

        // TODO: Is it necessary to create default schema in cluster mode?
        info!("server start, create default schema if not exist");
        self.create_default_schema_if_not_exists().await;

        info!("server start, start services");
        self.http_service.start().await.context(HttpService { msg: "start failed", })?;

        self.mysql_service.start().await.context(StartMysqlService)?;

        self.rpc_services.start().await.context(StartGrpcService)?;

        info!("server start finished");

        Ok(())
    }

    async fn create_default_schema_if_not_exists(&self) {
        let catalog_mgr = &self.instance.catalog_manager;
        let default_catalog = catalog_mgr
            .catalog_by_name(catalog_mgr.default_catalog_name())
            .expect("Fail to retrieve default catalog")
            .expect("Default catalog doesn't exist");

        if default_catalog
            .schema_by_name(catalog_mgr.default_schema_name())
            .expect("Fail to retrieve default schema")
            .is_none()
        {
            warn!("Default schema doesn't exist and create it");
            default_catalog
                .create_schema(catalog_mgr.default_schema_name())
                .await
                .expect("Fail to create default schema");
        }
    }
}

#[must_use]
pub struct Builder<Q> {
    server_config: ServerConfig,
    remote_engine_client_config: remote_engine_client::config::Config,
    node_addr: String,
    config_content: Option<String>,
    engine_runtimes: Option<Arc<EngineRuntimes>>,
    log_runtime: Option<Arc<RuntimeLevel>>,
    catalog_manager: Option<ManagerRef>,
    query_executor: Option<Q>,
    table_engine: Option<TableEngineRef>,
    table_manipulator: Option<TableManipulatorRef>,
    function_registry: Option<FunctionRegistryRef>,
    limiter: Limiter,
    cluster: Option<ClusterRef>,
    router: Option<RouterRef>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
    local_tables_recoverer: Option<LocalTablesRecoverer>,
    opened_wals: Option<OpenedWals>,
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn new(serverConfig: ServerConfig) -> Self {
        Self {
            server_config: serverConfig,
            remote_engine_client_config: remote_engine_client::Config::default(),
            node_addr: "".to_string(),
            config_content: None,
            engine_runtimes: None,
            log_runtime: None,
            catalog_manager: None,
            query_executor: None,
            table_engine: None,
            table_manipulator: None,
            function_registry: None,
            limiter: Limiter::default(),
            cluster: None,
            router: None,
            schema_config_provider: None,
            local_tables_recoverer: None,
            opened_wals: None,
        }
    }

    pub fn node_addr(mut self, node_addr: String) -> Self {
        self.node_addr = node_addr;
        self
    }

    pub fn config_content(mut self, config_content: String) -> Self {
        self.config_content = Some(config_content);
        self
    }

    pub fn engine_runtimes(mut self, engine_runtimes: Arc<EngineRuntimes>) -> Self {
        self.engine_runtimes = Some(engine_runtimes);
        self
    }

    pub fn log_runtime(mut self, log_runtime: Arc<RuntimeLevel>) -> Self {
        self.log_runtime = Some(log_runtime);
        self
    }

    pub fn catalog_manager(mut self, val: ManagerRef) -> Self {
        self.catalog_manager = Some(val);
        self
    }

    pub fn query_executor(mut self, val: Q) -> Self {
        self.query_executor = Some(val);
        self
    }

    pub fn table_engine(mut self, val: TableEngineRef) -> Self {
        self.table_engine = Some(val);
        self
    }

    pub fn table_manipulator(mut self, val: TableManipulatorRef) -> Self {
        self.table_manipulator = Some(val);
        self
    }

    pub fn function_registry(mut self, val: FunctionRegistryRef) -> Self {
        self.function_registry = Some(val);
        self
    }

    pub fn limiter(mut self, val: Limiter) -> Self {
        self.limiter = val;
        self
    }

    pub fn cluster(mut self, cluster: ClusterRef) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    pub fn schema_config_provider(
        mut self,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        self.schema_config_provider = Some(schema_config_provider);
        self
    }

    pub fn local_tables_recoverer(mut self, local_tables_recoverer: LocalTablesRecoverer) -> Self {
        self.local_tables_recoverer = Some(local_tables_recoverer);
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }

    /// Build and run the server
    pub fn build(self) -> Result<Server<Q>> {
        // Build instance
        let catalog_manager = self.catalog_manager.context(MissingCatalogManager)?;
        let query_executor = self.query_executor.context(MissingQueryExecutor)?;
        let table_engine = self.table_engine.context(MissingTableEngine)?;
        let table_manipulator = self.table_manipulator.context(MissingTableManipulator)?;
        let function_registry = self.function_registry.context(MissingFunctionRegistry)?;
        let opened_wals = self.opened_wals.context(MissingWals)?;
        let router = self.router.context(MissingRouter)?;
        let provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;
        let log_runtime = self.log_runtime.context(MissingLogRuntime)?;
        let engine_runtimes = self.engine_runtimes.context(MissingEngineRuntimes)?;
        let config_content = self.config_content.expect("Missing config content");

        let remote_engine_ref = Arc::new(RemoteEngineImpl::new(
            self.remote_engine_client_config.clone(),
            router.clone(),
            engine_runtimes.io_runtime.clone(),
        ));

        let partition_table_engine = Arc::new(PartitionTableEngine::new(remote_engine_ref.clone()));

        let instance = {
            InstanceRef::new(Instance {
                catalog_manager,
                query_executor,
                table_engine,
                partition_table_engine,
                function_registry,
                limiter: self.limiter,
                table_manipulator,
                remote_engine_ref,
            })
        };

        let grpc_endpoint = Endpoint {
            addr: self.server_config.bind_addr.clone(),
            port: self.server_config.grpc_port,
        };

        // Create http config
        let http_endpoint = Endpoint {
            addr: self.server_config.bind_addr.clone(),
            port: self.server_config.http_port,
        };

        let http_config = HttpConfig {
            endpoint: http_endpoint,
            max_body_size: self.server_config.http_max_body_size.as_byte(),
            timeout: self.server_config.timeout.map(|v| v.0),
        };

        let proxy =
            Arc::new(Proxy::new(router.clone(),
                                instance.clone(),
                                self.server_config.forward,
                                Endpoint::new(self.node_addr, self.server_config.grpc_port),
                                self.server_config.resp_compress_min_length.as_byte() as usize,
                                self.server_config.auto_create_table,
                                provider.clone(),
                                self.server_config.hotspot,
                                engine_runtimes.clone(),
                                self.cluster.is_some()));

        let http_service = http::Builder::new(http_config)
            .engine_runtimes(engine_runtimes.clone())
            .log_runtime(log_runtime)
            .config_content(config_content)
            .cluster(self.cluster.clone())
            .proxy(proxy.clone())
            .opened_wals(opened_wals.clone())
            .build()
            .context(HttpService {
                msg: "build failed",
            })?;

        let mysql_config = mysql::MysqlConfig {
            ip: self.server_config.bind_addr.clone(),
            port: self.server_config.mysql_port,
            timeout: self.server_config.timeout.map(|v| v.0),
        };

        let mysql_service = mysql::Builder::new(mysql_config)
            .runtimes(engine_runtimes.clone())
            .proxy(proxy.clone())
            .build()
            .context(BuildMysqlService)?;

        let rpc_services = grpc::Builder::new()
            .endpoint(grpc_endpoint.to_string())
            .runtimes(engine_runtimes)
            .instance(instance.clone())
            .cluster(self.cluster.clone())
            .opened_wals(opened_wals)
            .timeout(self.server_config.timeout.map(|v| v.0))
            .proxy(proxy)
            .request_notifiers(self.server_config.enable_query_dedup)
            .build()
            .context(BuildGrpcService)?;

        let server = Server {
            http_service,
            rpc_services,
            mysql_service,
            instance,
            cluster: self.cluster,
            local_tables_recoverer: self.local_tables_recoverer,
        };
        Ok(server)
    }
}

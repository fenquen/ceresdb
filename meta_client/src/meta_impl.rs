// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::{
    common::ResponseHeader,
    meta_service::{self, ceresmeta_rpc_service_client::CeresmetaRpcServiceClient},
};
use generic_error::BoxError;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use time_ext::ReadableDuration;

use crate::{
    types::{
        AllocSchemaIdRequest, AllocSchemaIdResponse, CreateTableRequest, CreateTableResponse,
        DropTableRequest, DropTableResponse, GetNodesRequest, GetNodesResponse,
        GetTablesOfShardsRequest, GetTablesOfShardsResponse, NodeInfo, NodeMetaInfo, RequestHeader,
        RouteTablesRequest, RouteTablesResponse, ShardInfo,
    },
    BadResponse, FailAllocSchemaId, FailConnect, FailCreateTable, FailDropTable, FailGetTables,
    FailRouteTables, FailSendHeartbeat, MetaClient, MissingHeader, Result,
};

type MetaServiceGrpcClient = CeresmetaRpcServiceClient<tonic::transport::Channel>;

#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster_name: String,
    pub meta_addr: String,
    pub lease: ReadableDuration,
    pub timeout: ReadableDuration,
    pub cq_count: usize,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster_name: String::new(),
            meta_addr: "127.0.0.1:8080".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
        }
    }
}

/// Default meta client impl, will interact with a remote meta node.
pub struct MetaClientImpl {
    config: MetaClientConfig,
    node_meta_info: NodeMetaInfo,
    client: MetaServiceGrpcClient,
}

impl MetaClientImpl {
    pub async fn connect(config: MetaClientConfig, node_meta_info: NodeMetaInfo) -> Result<Self> {
        let client = {
            let endpoint = tonic::transport::Endpoint::from_shared(config.meta_addr.to_string())
                .box_err()
                .context(FailConnect {
                    addr: &config.meta_addr,
                })?
                .timeout(config.timeout.0);
            MetaServiceGrpcClient::connect(endpoint)
                .await
                .box_err()
                .context(FailConnect {
                    addr: &config.meta_addr,
                })?
        };

        Ok(Self {
            config,
            node_meta_info,
            client,
        })
    }

    fn request_header(&self) -> RequestHeader {
        RequestHeader {
            node: self.node_meta_info.endpoint(),
            cluster_name: self.config.cluster_name.clone(),
        }
    }

    #[inline]
    fn client(&self) -> MetaServiceGrpcClient {
        self.client.clone()
    }
}

#[async_trait]
impl MetaClient for MetaClientImpl {
    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse> {
        let mut pb_req = meta_service::AllocSchemaIdRequest::from(req);
        pb_req.header = Some(self.request_header().into());

        info!("Meta client try to alloc schema id, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .alloc_schema_id(pb_req)
            .await
            .box_err()
            .context(FailAllocSchemaId)?
            .into_inner();

        info!(
            "Meta client finish allocating schema id, resp:{:?}",
            pb_resp
        );

        check_response_header(&pb_resp.header)?;
        Ok(AllocSchemaIdResponse::from(pb_resp))
    }

    async fn create_table(&self, req: CreateTableRequest) -> Result<CreateTableResponse> {
        let mut pb_req = meta_service::CreateTableRequest::from(req);
        pb_req.header = Some(self.request_header().into());

        info!("Meta client try to create table, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .create_table(pb_req)
            .await
            .box_err()
            .context(FailCreateTable)?
            .into_inner();

        info!("Meta client finish creating table, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)?;
        CreateTableResponse::try_from(pb_resp)
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse> {
        let mut pb_req = meta_service::DropTableRequest::from(req.clone());
        pb_req.header = Some(self.request_header().into());

        info!("Meta client try to drop table, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .drop_table(pb_req)
            .await
            .box_err()
            .context(FailDropTable)?
            .into_inner();

        info!("Meta client finish dropping table, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)?;
        DropTableResponse::try_from(pb_resp)
    }

    async fn get_tables_of_shards(
        &self,
        req: GetTablesOfShardsRequest,
    ) -> Result<GetTablesOfShardsResponse> {
        let mut pb_req = meta_service::GetTablesOfShardsRequest::from(req);
        pb_req.header = Some(self.request_header().into());

        debug!("Meta client try to get tables, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .get_tables_of_shards(pb_req)
            .await
            .box_err()
            .context(FailGetTables)?
            .into_inner();

        debug!("Meta client finish getting tables, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)?;

        GetTablesOfShardsResponse::try_from(pb_resp)
    }

    async fn route_tables(&self, req: RouteTablesRequest) -> Result<RouteTablesResponse> {
        let mut pb_req = meta_service::RouteTablesRequest::from(req);
        pb_req.header = Some(self.request_header().into());

        debug!("Meta client try to route tables, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .route_tables(pb_req)
            .await
            .box_err()
            .context(FailRouteTables)?
            .into_inner();

        debug!("Meta client finish routing tables, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)?;
        RouteTablesResponse::try_from(pb_resp)
    }

    async fn get_nodes(&self, req: GetNodesRequest) -> Result<GetNodesResponse> {
        let mut pb_req = meta_service::GetNodesRequest::from(req);
        pb_req.header = Some(self.request_header().into());

        debug!("Meta client try to get nodes, req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .get_nodes(pb_req)
            .await
            .box_err()
            .context(FailRouteTables)?
            .into_inner();

        debug!("Meta client finish getting nodes, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)?;
        GetNodesResponse::try_from(pb_resp)
    }

    async fn send_heartbeat(&self, shard_infos: Vec<ShardInfo>) -> Result<()> {
        let node_info = NodeInfo {
            node_meta_info: self.node_meta_info.clone(),
            shard_infos,
        };
        let pb_req = meta_service::NodeHeartbeatRequest {
            header: Some(self.request_header().into()),
            info: Some(node_info.into()),
        };

        info!("Meta client try to send heartbeat req:{:?}", pb_req);

        let pb_resp = self
            .client()
            .node_heartbeat(pb_req)
            .await
            .box_err()
            .context(FailSendHeartbeat {
                cluster: &self.config.cluster_name,
            })?
            .into_inner();

        info!("Meta client finish sending heartbeat, resp:{:?}", pb_resp);

        check_response_header(&pb_resp.header)
    }
}

fn check_response_header(header: &Option<ResponseHeader>) -> Result<()> {
    let header = header.as_ref().context(MissingHeader)?;
    if header.code == 0 {
        Ok(())
    } else {
        BadResponse {
            code: header.code,
            msg: header.error.clone(),
        }
            .fail()
    }
}

/// Create a meta client with given `config`.
pub async fn build_meta_client(config: MetaClientConfig,
                               node_meta_info: NodeMetaInfo) -> Result<Arc<dyn MetaClient>> {
    let meta_client = MetaClientImpl::connect(config, node_meta_info).await?;
    Ok(Arc::new(meta_client))
}

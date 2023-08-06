// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use generic_error::BoxError;
use log::trace;
use meta_client::types::RouteTablesRequest;
use moka::future::Cache;
use snafu::ResultExt;

use crate::{
    endpoint::Endpoint, OtherWithCause, ParseEndpoint, Result, RouteCacheConfig, Router, TableInfo,
};

#[derive(Clone, Debug)]
struct RouteData {
    table_info: TableInfo,
    endpoint: Option<Endpoint>,
}

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
    cache: Option<Cache<String, RouteData>>,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef, cache_config: RouteCacheConfig) -> Self {
        let cache = if cache_config.enable {
            Some(
                Cache::builder()
                    .time_to_live(cache_config.ttl.0)
                    .time_to_idle(cache_config.tti.0)
                    .max_capacity(cache_config.capacity)
                    .build(),
            )
        } else {
            None
        };

        Self { cluster, cache }
    }

    /// route table from local cache, return cache routes and tables which are
    /// not in cache
    fn route_from_cache(&self, tables: &[String], routes: &mut Vec<RouteData>) -> Vec<String> {
        let mut miss = vec![];

        if let Some(cache) = &self.cache {
            for table in tables {
                if let Some(route) = cache.get(table) {
                    routes.push(route.clone());
                } else {
                    miss.push(table.clone());
                }
            }
        } else {
            miss = tables.to_vec();
        }

        miss
    }

    async fn route_with_cache(
        &self,
        tables: &Vec<String>,
        database: String,
    ) -> Result<Vec<RouteData>> {
        // Firstly route table from local cache.
        let mut routes = Vec::with_capacity(tables.len());
        let miss = self.route_from_cache(tables, &mut routes);
        trace!("Route from cache, miss:{miss:?}, routes:{routes:?}");

        if miss.is_empty() {
            return Ok(routes);
        }
        let route_tables_req = RouteTablesRequest {
            schema_name: database,
            table_names: miss,
        };

        let route_resp = self
            .cluster
            .route_tables(&route_tables_req)
            .await
            .box_err()
            .with_context(|| OtherWithCause {
                msg: format!("Failed to route tables by cluster, req:{route_tables_req:?}"),
            })?;
        trace!("Route tables by cluster, req:{route_tables_req:?}, resp:{route_resp:?}");

        // Now we pick up the nodes who own the leader shard for the route response.
        for (table_name, route_entry) in route_resp.entries {
            let route = if route_entry.node_shards.is_empty() {
                Some(make_route(route_entry.table_info, None)?)
            } else {
                route_entry
                    .node_shards
                    .into_iter()
                    .find(|node_shard| node_shard.shard_info.is_leader())
                    .map(|node_shard| {
                        make_route(route_entry.table_info, Some(&node_shard.endpoint))
                    })
                    .transpose()?
            };

            if let Some(route) = route {
                if let Some(cache) = &self.cache {
                    // There may be data race here, and it is acceptable currently.
                    cache.insert(table_name.clone(), route.clone()).await;
                }
                routes.push(route);
            }
        }
        Ok(routes)
    }
}

/// Make a route according to the table_info and the raw endpoint.
fn make_route(table_info: TableInfo, endpoint: Option<&str>) -> Result<RouteData> {
    let endpoint = endpoint
        .map(|v| v.parse().context(ParseEndpoint { endpoint: v }))
        .transpose()?;

    Ok(RouteData {
        table_info,
        endpoint,
    })
}

#[async_trait]
impl Router for ClusterBasedRouter {
    async fn route(&self, req: RouteRequest) -> Result<Vec<Route>> {
        let req_ctx = req.context.unwrap();
        let route_data_vec = self.route_with_cache(&req.tables, req_ctx.database).await?;
        Ok(route_data_vec
            .into_iter()
            .map(|v| Route {
                table: v.table_info.name,
                endpoint: v.endpoint.map(Into::into),
            })
            .collect())
    }

    async fn fetch_table_info(&self, schema: &str, table: &str) -> Result<Option<TableInfo>> {
        let mut route_data_vec = self
            .route_with_cache(&vec![table.to_string()], schema.to_string())
            .await?;
        if route_data_vec.is_empty() {
            return Ok(None);
        }

        let route_data = route_data_vec.remove(0);
        let table_info = route_data.table_info;
        Ok(Some(table_info))
    }
}

// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Forward for grpc services
use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, RequestContext, RouteRequest,
};
use log::{debug, error, warn};
use macros::define_result;
use router::{endpoint::Endpoint, RouterRef};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use time_ext::ReadableDuration;
use tonic::{
    metadata::errors::InvalidMetadataValue,
    transport::{self, Channel},
};

use crate::FORWARDED_FROM;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid endpoint, endpoint:{}, err:{}.\nBacktrace:\n{}",
        endpoint,
        source,
        backtrace
    ))]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Local ip addr should not be loopback, addr:{}.\nBacktrace:\n{}",
        ip_addr,
        backtrace
    ))]
    LoopbackLocalIpAddr {
        ip_addr: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid schema, schema:{}, err:{}.\nBacktrace:\n{}",
        schema,
        source,
        backtrace
    ))]
    InvalidSchema {
        schema: String,
        source: InvalidMetadataValue,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to connect endpoint, endpoint:{}, err:{}.\nBacktrace:\n{}",
        endpoint,
        source,
        backtrace
    ))]
    Connect {
        endpoint: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Request should not be forwarded twice, forward from:{}", endpoint))]
    ForwardedErr { endpoint: String },
}

define_result!(Error);

pub type ForwarderRef = Arc<Forwarder<DefaultClientBuilder>>;
pub trait ForwarderRpc<Req, Resp, Err> = FnOnce(
    StorageServiceClient<Channel>,
    tonic::Request<Req>,
    &Endpoint,
) -> Box<
    dyn std::future::Future<Output = std::result::Result<Resp, Err>> + Send + Unpin,
>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Sets an interval for HTTP2 Ping frames should be sent to keep a
    /// connection alive.
    pub keep_alive_interval: ReadableDuration,
    /// A timeout for receiving an acknowledgement of the keep-alive ping
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed
    pub keep_alive_timeout: ReadableDuration,
    /// default keep http2 connections alive while idle
    pub keep_alive_while_idle: bool,
    pub connect_timeout: ReadableDuration,
    pub forward_timeout: Option<ReadableDuration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive_interval: ReadableDuration::secs(60 * 10),
            keep_alive_timeout: ReadableDuration::secs(3),
            keep_alive_while_idle: true,
            connect_timeout: ReadableDuration::secs(3),
            forward_timeout: None,
        }
    }
}

#[async_trait]
pub trait ClientBuilder {
    async fn connect(&self, endpoint: &Endpoint) -> Result<StorageServiceClient<Channel>>;
}

pub struct DefaultClientBuilder {
    config: Config,
}

impl DefaultClientBuilder {
    #[inline]
    fn make_endpoint_with_scheme(endpoint: &Endpoint) -> String {
        format!("http://{}:{}", endpoint.addr, endpoint.port)
    }
}

#[async_trait]
impl ClientBuilder for DefaultClientBuilder {
    async fn connect(&self, endpoint: &Endpoint) -> Result<StorageServiceClient<Channel>> {
        let endpoint_with_scheme = Self::make_endpoint_with_scheme(endpoint);
        let configured_endpoint = transport::Endpoint::from_shared(endpoint_with_scheme.clone())
            .context(InvalidEndpoint {
                endpoint: &endpoint_with_scheme,
            })?;

        let configured_endpoint = match self.config.keep_alive_while_idle {
            true => configured_endpoint
                .connect_timeout(self.config.connect_timeout.0)
                .keep_alive_timeout(self.config.keep_alive_timeout.0)
                .keep_alive_while_idle(true)
                .http2_keep_alive_interval(self.config.keep_alive_interval.0),
            false => configured_endpoint
                .connect_timeout(self.config.connect_timeout.0)
                .keep_alive_while_idle(false),
        };
        let channel = configured_endpoint.connect().await.context(Connect {
            endpoint: &endpoint_with_scheme,
        })?;

        let client = StorageServiceClient::new(channel);
        Ok(client)
    }
}

/// Forwarder does request forwarding.
///
/// No forward happens if the router tells the target endpoint is the same as
/// the local endpoint.
///
/// Assuming client wants to access some table which are located on server1 (the
/// router can tell the location information). Then here is the diagram
/// describing what the forwarder does:
///  peer-to-peer procedure: client --> server1
///  forwarding procedure:   client --> server0 (forwarding server) --> server1
pub struct Forwarder<B> {
    config: Config,
    router: RouterRef,
    local_endpoint: Endpoint,
    client_builder: B,
    clients: RwLock<HashMap<Endpoint, StorageServiceClient<Channel>>>,
}

/// The result of forwarding.
///
/// If no forwarding happens, [`Local`] can be used.
pub enum ForwardResult<Resp, Err> {
    Local,
    Forwarded(std::result::Result<Resp, Err>),
}

#[derive(Debug)]
pub struct ForwardRequest<Req> {
    pub schema: String,
    pub table: String,
    pub req: tonic::Request<Req>,
    pub forwarded_from: Option<String>,
}

impl Forwarder<DefaultClientBuilder> {
    pub fn new(config: Config, router: RouterRef, local_endpoint: Endpoint) -> Self {
        let client_builder = DefaultClientBuilder {
            config: config.clone(),
        };

        Self::new_with_client_builder(config, router, local_endpoint, client_builder)
    }
}

impl<B> Forwarder<B> {
    #[inline]
    fn is_loopback_ip(ip_addr: &str) -> bool {
        ip_addr
            .parse::<Ipv4Addr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
    }

    /// Check whether the target endpoint is the same as the local endpoint.
    pub fn is_local_endpoint(&self, target: &Endpoint) -> bool {
        if &self.local_endpoint == target {
            return true;
        }

        if self.local_endpoint.port != target.port {
            return false;
        }

        // Only need to check the remote is loopback addr.
        Self::is_loopback_ip(&target.addr)
    }

    /// Release the client for the given endpoint.
    fn release_client(&self, endpoint: &Endpoint) -> Option<StorageServiceClient<Channel>> {
        let mut clients = self.clients.write().unwrap();
        clients.remove(endpoint)
    }
}

impl<B: ClientBuilder> Forwarder<B> {
    pub fn new_with_client_builder(
        config: Config,
        router: RouterRef,
        local_endpoint: Endpoint,
        client_builder: B,
    ) -> Self {
        Self {
            config,
            local_endpoint,
            router,
            clients: RwLock::new(HashMap::new()),
            client_builder,
        }
    }

    /// Forward the request according to the configured router.
    ///
    /// Error will be thrown if it happens in the forwarding procedure, that is
    /// to say, some errors like the output from the `do_rpc` will be
    /// wrapped in the [`ForwardResult::Forwarded`].
    pub async fn forward<Req, Resp, Err, F>(
        &self,
        forward_req: ForwardRequest<Req>,
        do_rpc: F,
    ) -> Result<ForwardResult<Resp, Err>>
    where
        F: ForwarderRpc<Req, Resp, Err>,
        Req: std::fmt::Debug + Clone,
    {
        let ForwardRequest {
            schema,
            table,
            req,
            forwarded_from,
        } = forward_req;

        let route_req = RouteRequest {
            context: Some(RequestContext { database: schema }),
            tables: vec![table],
        };

        let endpoint = match self.router.route(route_req).await {
            Ok(mut routes) => {
                if routes.len() != 1 || routes[0].endpoint.is_none() {
                    warn!(
                        "Fail to forward request for multiple or empty route results, routes result:{:?}, req:{:?}",
                        routes, req
                    );
                    return Ok(ForwardResult::Local);
                }

                Endpoint::from(routes.remove(0).endpoint.unwrap())
            }
            Err(e) => {
                error!("Fail to route request, req:{:?}, err:{}", req, e);
                return Ok(ForwardResult::Local);
            }
        };

        self.forward_with_endpoint(endpoint, req, forwarded_from, do_rpc)
            .await
    }

    pub async fn forward_with_endpoint<Req, Resp, Err, F>(
        &self,
        endpoint: Endpoint,
        mut req: tonic::Request<Req>,
        forwarded_from: Option<String>,
        do_rpc: F,
    ) -> Result<ForwardResult<Resp, Err>>
    where
        F: ForwarderRpc<Req, Resp, Err>,
        Req: std::fmt::Debug + Clone,
    {
        if self.is_local_endpoint(&endpoint) {
            return Ok(ForwardResult::Local);
        }

        // Update the request.
        {
            if let Some(timeout) = self.config.forward_timeout {
                req.set_timeout(timeout.0);
            }
        }

        // TODO: add metrics to record the forwarding.
        debug!(
            "Try to forward request to {:?}, request:{:?}",
            endpoint, req,
        );

        if let Some(endpoint) = forwarded_from {
            return ForwardedErr { endpoint }.fail();
        }

        // mark forwarded
        req.metadata_mut().insert(
            FORWARDED_FROM,
            self.local_endpoint.to_string().parse().unwrap(),
        );

        let client = self.get_or_create_client(&endpoint).await?;
        match do_rpc(client, req, &endpoint).await {
            Err(e) => {
                // Release the grpc client for the error doesn't belong to the normal error.
                self.release_client(&endpoint);
                Ok(ForwardResult::Forwarded(Err(e)))
            }
            Ok(resp) => Ok(ForwardResult::Forwarded(Ok(resp))),
        }
    }

    async fn get_or_create_client(
        &self,
        endpoint: &Endpoint,
    ) -> Result<StorageServiceClient<Channel>> {
        {
            let clients = self.clients.read().unwrap();
            if let Some(v) = clients.get(endpoint) {
                return Ok(v.clone());
            }
        }

        let new_client = self.client_builder.connect(endpoint).await?;
        {
            let mut clients = self.clients.write().unwrap();
            if let Some(v) = clients.get(endpoint) {
                return Ok(v.clone());
            }
            clients.insert(endpoint.clone(), new_client.clone());
        }

        Ok(new_client)
    }
}
// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Manage meta data of the engine

pub mod details;
pub mod meta_edit;
pub mod meta_snapshot;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use common_types::table::ShardId;
use generic_error::GenericResult;
use table_engine::table::TableId;

use crate::{manifest::meta_edit::MetaEditRequest, space::SpaceId};

#[derive(Debug)]
pub struct LoadRequest {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub shard_id: ShardId,
}

pub type SnapshotRequest = LoadRequest;

/// Manifest holds meta data of all tables
#[async_trait]
pub trait Manifest: Send + Sync + fmt::Debug {
    /// Apply edit to table metas, store it to storage.
    async fn apply_edit(&self, metaEditRequest: MetaEditRequest) -> GenericResult<()>;

    /// Recover table metas from storage.
    async fn recover(&self, loadRequest: &LoadRequest) -> GenericResult<()>;

    async fn do_snapshot(&self, request: SnapshotRequest) -> GenericResult<()>;
}

pub type ManifestRef = Arc<dyn Manifest>;

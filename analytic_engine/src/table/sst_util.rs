// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::iter::FromIterator;

use object_store::Path;
use table_engine::table::TableId;

use crate::{space::SpaceId, sst::manager::FileId};

const SST_FILE_SUFFIX: &str = "sst";

pub fn buildSstFilePath(space_id: SpaceId, table_id: TableId, file_id: FileId) -> Path {
    Path::from_iter([space_id.to_string(), table_id.to_string(), format!("{}.{}", file_id, SST_FILE_SUFFIX)])
}

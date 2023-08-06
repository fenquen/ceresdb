// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contains common types

pub mod column;
pub mod column_schema;
pub mod datum;
pub(crate) mod hex;
pub mod projected_schema;
pub mod record_batch;
pub mod request_id;
pub mod row;
pub mod schema;
pub mod string;
pub mod table;
pub mod time;

/// Sequence number
pub type SequenceNumber = u64;
/// Maximum sequence number, all sequence number should less than this.
pub const MAX_SEQUENCE_NUMBER: u64 = u64::MAX;
/// Minimum sequence number, all sequence number should greater than this, so
/// sequence number should starts from 1.
pub const MIN_SEQUENCE_NUMBER: u64 = 0;

/// Enable ttl key
pub const OPTION_KEY_ENABLE_TTL: &str = "enable_ttl";
pub const SEGMENT_DURATION: &str = "segment_duration";
pub const ENABLE_TTL: &str = OPTION_KEY_ENABLE_TTL;
pub const TTL: &str = "ttl";
pub const ARENA_BLOCK_SIZE: &str = "arena_block_size";
pub const WRITE_BUFFER_SIZE: &str = "write_buffer_size";
pub const COMPACTION_STRATEGY: &str = "compaction_strategy";
pub const NUM_ROWS_PER_ROW_GROUP: &str = "num_rows_per_row_group";
pub const UPDATE_MODE: &str = "update_mode";
pub const COMPRESSION: &str = "compression";
pub const STORAGE_FORMAT: &str = "storage_format";
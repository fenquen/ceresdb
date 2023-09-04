// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader trait definition.

use async_trait::async_trait;
use common_types::record_batch::RecordBatchWithKey;

use crate::{prefetchable_stream::PrefetchableStream, sst::meta_data::SstMetaData};

pub mod error {
    use generic_error::GenericError;
    use macros::define_result;
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("try to read again, path:{path}.\nBacktrace:\n{backtrace}"))]
        ReadAgain { backtrace: Backtrace, path: String },

        #[snafu(display("Fail to read persisted file, path:{path}, err:{source}"))]
        ReadPersist { path: String, source: GenericError },

        #[snafu(display("Failed to decode record batch, err:{source}"))]
        DecodeRecordBatch { source: GenericError },

        #[snafu(display("failed to decode sst meta data, file_path:{file_path}, err:{source}.\nBacktrace:\n{backtrace:?}", ))]
        FetchAndDecodeSstMeta {
            file_path: String,
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display("failed to decode page indexes for meta data, file_path:{file_path}, err:{source}.\nBacktrace:\n{backtrace:?}", ))]
        DecodePageIndexes {
            file_path: String,
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to decode sst meta data, err:{source}"))]
        DecodeSstMeta { source: GenericError },

        #[snafu(display("Sst meta data is not found.\nBacktrace:\n{backtrace}"))]
        SstMetaNotFound { backtrace: Backtrace },

        #[snafu(display("Fail to projection, err:{source}"))]
        Projection { source: GenericError },

        #[snafu(display("Sst meta data is empty.\nBacktrace:\n{backtrace}"))]
        EmptySstMeta { backtrace: Backtrace },

        #[snafu(display("Invalid schema, err:{source}"))]
        InvalidSchema { source: common_types::schema::Error },

        #[snafu(display("Meet a datafusion error, err:{source}\nBacktrace:\n{backtrace}"))]
        DataFusionError {
            source: datafusion::error::DataFusionError,
            backtrace: Backtrace,
        },

        #[snafu(display("Meet a object store error, err:{source}\nBacktrace:\n{backtrace}"))]
        ObjectStoreError {
            source: object_store::ObjectStoreError,
            backtrace: Backtrace,
        },

        #[snafu(display("Meet a parquet error, err:{source}\nBacktrace:\n{backtrace}"))]
        ParquetError {
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display("Other kind of error:{source}"))]
        Other { source: GenericError },

        #[snafu(display("Other kind of error, msg:{msg}.\nBacktrace:\n{backtrace}"))]
        OtherNoCause { msg: String, backtrace: Backtrace },
    }

    define_result!(Error);
}

pub use error::*;

#[async_trait]
pub trait SstReader {
    async fn meta_data(&mut self) -> Result<SstMetaData>;

    async fn read(&mut self) -> Result<Box<dyn PrefetchableStream<Item=Result<RecordBatchWithKey>>>>;
}

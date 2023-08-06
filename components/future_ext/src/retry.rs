// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Util function to retry future.

use std::time::Duration;

use futures::Future;

// TODO: add backoff
// https://github.com/apache/arrow-rs/blob/dfb642809e93c2c1b8343692f4e4b3080000f988/object_store/src/client/backoff.rs#L26
pub struct RetryConfig {
    pub max_retries: usize,
    pub interval: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            interval: Duration::from_millis(500),
        }
    }
}

pub async fn retry_async<F, Fut, T, E>(f: F, config: &RetryConfig) -> Fut::Output
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    for _ in 0..config.max_retries {
        let result = f().await;

        if result.is_ok() {
            return result;
        }
        tokio::time::sleep(config.interval).await;
    }

    f().await
}
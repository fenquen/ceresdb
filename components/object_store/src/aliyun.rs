// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions, RetryConfig,
};

use crate::config::AliyunOptions;

fn normalize_endpoint(endpoint: &str, bucket: &str) -> String {
    if endpoint.starts_with("https") {
        format!(
            "https://{}.{}",
            bucket,
            endpoint.replacen("https://", "", 1)
        )
    } else {
        format!("http://{}.{}", bucket, endpoint.replacen("http://", "", 1))
    }
}

pub fn try_new(aliyun_opts: &AliyunOptions) -> upstream::Result<AmazonS3> {
    let cli_opt = ClientOptions::new()
        .with_allow_http(true)
        .with_pool_max_idle_per_host(aliyun_opts.http.pool_max_idle_per_host)
        .with_http2_keep_alive_timeout(aliyun_opts.http.keep_alive_timeout.0)
        .with_http2_keep_alive_while_idle()
        .with_http2_keep_alive_interval(aliyun_opts.http.keep_alive_interval.0)
        .with_timeout(aliyun_opts.http.timeout.0);
    let retry_config = RetryConfig {
        max_retries: aliyun_opts.retry.max_retries,
        retry_timeout: aliyun_opts.retry.retry_timeout.0,
        ..Default::default()
    };

    let endpoint = &aliyun_opts.endpoint;
    let bucket = &aliyun_opts.bucket;
    let endpoint = normalize_endpoint(endpoint, bucket);
    AmazonS3Builder::new()
        .with_virtual_hosted_style_request(true)
        // region is not used when virtual_hosted_style is true,
        // but is required, so dummy is used here
        // https://github.com/apache/arrow-rs/issues/3827
        .with_region("dummy")
        .with_access_key_id(&aliyun_opts.key_id)
        .with_secret_access_key(&aliyun_opts.key_secret)
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_client_options(cli_opt)
        .with_retry(retry_config)
        .build()
}
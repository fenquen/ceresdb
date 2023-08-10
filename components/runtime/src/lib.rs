// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A multi-threaded runtime that supports running Futures
#![allow(non_snake_case)]

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use macros::define_result;
use metrics::Metrics;
use pin_project_lite::pin_project;
use snafu::{Backtrace, GenerateBacktrace, ResultExt, Snafu};
use tokio::{
    runtime::{Builder as RuntimeBuilder, Runtime as TokioRuntime},
    task::{JoinError, JoinHandle as TokioJoinHandle},
};

mod metrics;

// TODO(yingwen): Use opaque error type
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("runtime Failed to build runtime, err:{}.\nBacktrace:\n{}", source, backtrace))]
    BuildRuntime {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("runtime Failed to join task, err:{}.\nBacktrace:\n{}", source, backtrace))]
    JoinTask {
        source: JoinError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub type RuntimeRef = Arc<Runtime>;

/// A runtime to run future tasks
#[derive(Debug)]
pub struct Runtime {
    tokioRuntime: TokioRuntime,
    metrics: Arc<Metrics>,
}

impl Runtime {
    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output> where F: Future + Send + 'static,
                                                                     F::Output: Send + 'static, {
        JoinHandle {
            inner: self.tokioRuntime.spawn(future),
        }
    }

    /// Run the provided function on an executor dedicated to blocking operations.
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R> where F: FnOnce() -> R + Send + 'static,
                                                                       R: Send + 'static, {
        JoinHandle { inner: self.tokioRuntime.spawn_blocking(func) }
    }

    /// Run a future to complete, this is the runtime's entry point
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.tokioRuntime.block_on(future)
    }

    /// Returns the runtime stats
    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            alive_thread_num: self.metrics.thread_alive_gauge.get(),
            idle_thread_num: self.metrics.thread_idle_gauge.get(),
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct JoinHandle<T> {
        #[pin]
        inner: TokioJoinHandle<T>,
    }
}

impl<T> JoinHandle<T> {
    pub fn abort(&self) {
        self.inner.abort();
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(ctx).map_err(|source| Error::JoinTask {
            source,
            backtrace: Backtrace::generate(),
        })
    }
}

/// Helper that aborts the given join handles on drop.
///
/// Useful to kill background tasks when the consumer is dropped.
#[derive(Debug)]
pub struct AbortOnDropMany<T>(pub Vec<JoinHandle<T>>);

impl<T> Drop for AbortOnDropMany<T> {
    fn drop(&mut self) {
        for join_handle in &self.0 {
            join_handle.inner.abort();
        }
    }
}

/// Runtime statistics
pub struct RuntimeStats {
    pub alive_thread_num: i64,
    pub idle_thread_num: i64,
}

pub struct Builder {
    thread_name: String,
    tokioRuntimeBuilder: RuntimeBuilder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            thread_name: "runtime-worker".to_string(),
            tokioRuntimeBuilder: RuntimeBuilder::new_multi_thread(),
        }
    }
}

fn with_metrics<F>(metrics: &Arc<Metrics>, f: F) -> impl Fn()
    where
        F: Fn(&Arc<Metrics>) + 'static,
{
    let m = metrics.clone();
    move || {
        f(&m);
    }
}

impl Builder {
    /// Sets the number of worker threads the Runtime will use.
    ///
    /// This can be any number above 0
    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        self.tokioRuntimeBuilder.worker_threads(val);
        self
    }

    /// Sets name of threads spawned by the Runtime thread pool
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.thread_name = val.into();
        self
    }

    /// Enable all feature of the underlying runtime
    pub fn enable_all(&mut self) -> &mut Self {
        self.tokioRuntimeBuilder.enable_all();
        self
    }

    pub fn build(&mut self) -> Result<Runtime> {
        let metrics = Arc::new(Metrics::new(&self.thread_name));

        let rt = self
            .tokioRuntimeBuilder
            .thread_name(self.thread_name.clone())
            .on_thread_start(with_metrics(&metrics, |m| {
                m.on_thread_start();
            }))
            .on_thread_stop(with_metrics(&metrics, |m| {
                m.on_thread_stop();
            }))
            .on_thread_park(with_metrics(&metrics, |m| {
                m.on_thread_park();
            }))
            .on_thread_unpark(with_metrics(&metrics, |m| {
                m.on_thread_unpark();
            }))
            .build()
            .context(BuildRuntime)?;

        Ok(Runtime { tokioRuntime: rt, metrics })
    }
}
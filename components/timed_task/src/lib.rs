// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Timed background tasks.

use std::{future::Future, time::Duration};

use log::info;
use runtime::{self, JoinHandle, Runtime};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time,
};

/// A task to run periodically.
pub struct TimedTask<B> {
    name: String,
    period: Duration,
    builder: B,
}

impl<B, Fut> TimedTask<B>
where
    B: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    pub fn start_timed_task(
        name: String,
        runtime: &Runtime,
        period: Duration,
        builder: B,
    ) -> TaskHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        let task = TimedTask {
            name,
            period,
            builder,
        };

        let handle = runtime.spawn(async move {
            task.run(rx).await;
        });
        TaskHandle {
            handle: Mutex::new(Some(handle)),
            sender: tx,
        }
    }

    async fn run(&self, mut rx: UnboundedReceiver<()>) {
        info!("TimedTask started, name:{}", self.name);

        loop {
            // TODO(yingwen): Maybe add a random offset to the peroid.
            match time::timeout(self.period, rx.recv()).await {
                Ok(_) => {
                    info!("TimedTask stopped, name:{}", self.name);

                    return;
                }
                Err(_) => {
                    let future = (self.builder)();
                    future.await;
                }
            }
        }
    }
}

/// Handle to the timed task.
///
/// The task will exit asynchronously after this handle is dropped.
pub struct TaskHandle {
    handle: Mutex<Option<JoinHandle<()>>>,
    sender: UnboundedSender<()>,
}

impl TaskHandle {
    /// Explicit stop the task and wait util the task exits.
    pub async fn stop_task(&self) -> std::result::Result<(), runtime::Error> {
        self.notify_exit();

        let handle = self.handle.lock().await.take();
        if let Some(h) = handle {
            h.await?;
        }

        Ok(())
    }

    fn notify_exit(&self) {
        if self.sender.send(()).is_err() {
            info!("The sender of task is disconnected");
        }
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.notify_exit();
    }
}
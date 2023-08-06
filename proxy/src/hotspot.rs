// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! hotspot recorder
use std::{fmt::Write, sync::Arc, time::Duration};

use ceresdbproto::storage::{
    PrometheusQueryRequest, RequestContext, SqlQueryRequest, WriteRequest,
};
use log::{info, warn};
use runtime::Runtime;
use serde::{Deserialize, Serialize};
use spin::Mutex as SpinMutex;
use timed_task::TimedTask;
use tokio::sync::mpsc::{self, Sender};

use crate::{hotspot_lru::HotspotLru, util};

type QueryKey = String;
type WriteKey = String;
const TAG: &str = "hotspot autodump";
const RECODER_CHANNEL_CAP: usize = 64 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Max items size for query hotspot
    query_cap: Option<usize>,
    /// Max items size for write hotspot
    write_cap: Option<usize>,
    dump_interval: Duration,
    auto_dump_interval: bool,
    /// Max items for dump hotspot
    auto_dump_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            query_cap: Some(10_000),
            write_cap: Some(10_000),
            dump_interval: Duration::from_secs(5),
            auto_dump_interval: true,
            auto_dump_len: 10,
        }
    }
}

enum Message {
    Query(QueryKey),
    Write {
        key: WriteKey,
        row_count: usize,
        field_count: usize,
    },
}

#[derive(Clone)]
pub struct HotspotRecorder {
    tx: Arc<Sender<Message>>,
    stat: HotspotStat,
}

#[derive(Clone)]
pub struct HotspotStat {
    hotspot_query: Option<Arc<SpinMutex<HotspotLru<QueryKey>>>>,
    hotspot_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
    hotspot_field_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
}

impl HotspotStat {
    /// return read count / write row count / write field count
    pub fn dump(&self) -> Dump {
        Dump {
            read_hots: self
                .pop_read_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
            write_hots: self
                .pop_write_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
            write_field_hots: self
                .pop_write_field_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
        }
    }

    fn format_hots(hots: Vec<(String, u64)>) -> Vec<String> {
        hots.into_iter()
            .map(|(k, v)| format!("metric={k}, heats={v}"))
            .collect()
    }

    fn pop_read_hots(&self) -> Option<Vec<(QueryKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_query)
    }

    fn pop_write_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_write)
    }

    fn pop_write_field_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_field_write)
    }

    fn pop_hots(target: &Option<Arc<SpinMutex<HotspotLru<String>>>>) -> Option<Vec<(String, u64)>> {
        target.as_ref().map(|hotspot| {
            let mut hots = hotspot.lock().pop_all();
            hots.sort_by(|a, b| b.1.cmp(&a.1));
            hots
        })
    }
}

#[derive(Clone)]
pub struct Dump {
    pub read_hots: Vec<String>,
    pub write_hots: Vec<String>,
    pub write_field_hots: Vec<String>,
}

impl HotspotRecorder {
    pub fn new(config: Config, runtime: Arc<Runtime>) -> Self {
        let hotspot_query = Self::init_lru(config.query_cap);
        let hotspot_write = Self::init_lru(config.write_cap);
        let hotspot_field_write = Self::init_lru(config.write_cap);

        let stat = HotspotStat {
            hotspot_query: hotspot_query.clone(),
            hotspot_write: hotspot_write.clone(),
            hotspot_field_write: hotspot_field_write.clone(),
        };

        let task_handle = if config.auto_dump_interval {
            let interval = config.dump_interval;
            let dump_len = config.auto_dump_len;
            let stat_clone = stat.clone();
            let builder = move || {
                let stat_in_builder = stat_clone.clone();
                async move {
                    let Dump {
                        read_hots,
                        write_hots,
                        write_field_hots,
                    } = stat_in_builder.dump();

                    read_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} query {}", TAG, hot));
                    write_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write rows {}", TAG, hot));
                    write_field_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write fields {}", TAG, hot));
                }
            };

            Some(TimedTask::start_timed_task(
                String::from("hotspot_dump"),
                &runtime,
                interval,
                builder,
            ))
        } else {
            None
        };

        let (tx, mut rx) = mpsc::channel(RECODER_CHANNEL_CAP);
        runtime.spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        warn!("Hotspot recoder sender stopped");
                        if let Some(handle) = task_handle {
                            handle.stop_task().await.unwrap();
                        }
                        break;
                    }
                    Some(msg) => match msg {
                        Message::Query(read_key) => {
                            if let Some(hotspot) = &hotspot_query {
                                hotspot.lock().inc(&read_key, 1);
                            }
                        }
                        Message::Write {
                            key,
                            row_count,
                            field_count,
                        } => {
                            if let Some(hotspot) = &hotspot_write {
                                hotspot.lock().inc(&key, row_count as u64);
                            }

                            if let Some(hotspot) = &hotspot_field_write {
                                hotspot.lock().inc(&key, field_count as u64);
                            }
                        }
                    },
                }
            }
        });

        Self {
            tx: Arc::new(tx),
            stat,
        }
    }

    #[inline]
    fn init_lru(cap: Option<usize>) -> Option<Arc<SpinMutex<HotspotLru<QueryKey>>>> {
        HotspotLru::new(cap?).map(|lru| Arc::new(SpinMutex::new(lru)))
    }

    fn key_prefix(context: &Option<RequestContext>) -> String {
        let mut prefix = String::new();
        match context {
            Some(ctx) => {
                // use database as prefix
                if !ctx.database.is_empty() {
                    write!(prefix, "{}/", ctx.database).unwrap();
                }
            }
            None => {}
        }

        prefix
    }

    #[inline]
    fn table_hot_key(context: &Option<RequestContext>, table: &String) -> String {
        let prefix = Self::key_prefix(context);
        prefix + table
    }

    pub async fn inc_sql_query_reqs(&self, req: &SqlQueryRequest) {
        if self.stat.hotspot_query.is_none() {
            return;
        }

        for table in &req.tables {
            self.send_msg_or_log(
                "inc_query_reqs",
                Message::Query(Self::table_hot_key(&req.context, table)),
            )
            .await;
        }
    }

    pub async fn inc_write_reqs(&self, req: &WriteRequest) {
        if self.stat.hotspot_write.is_some() && self.stat.hotspot_field_write.is_some() {
            for table_request in &req.table_requests {
                let hot_key = Self::table_hot_key(&req.context, &table_request.table);
                let mut row_count = 0;
                let mut field_count = 0;
                for entry in &table_request.entries {
                    row_count += 1;
                    for field_group in &entry.field_groups {
                        field_count += field_group.fields.len();
                    }
                }
                self.send_msg_or_log(
                    "inc_write_reqs",
                    Message::Write {
                        key: hot_key,
                        row_count,
                        field_count,
                    },
                )
                .await;
            }
        }
    }

    pub async fn inc_promql_reqs(&self, req: &PrometheusQueryRequest) {
        if self.stat.hotspot_query.is_none() {
            return;
        }

        if let Some(expr) = &req.expr {
            if let Some(table) = util::table_from_expr(expr) {
                let hot_key = Self::table_hot_key(&req.context, &table);
                self.send_msg_or_log("inc_query_reqs", Message::Query(hot_key))
                    .await
            }
        }
    }

    async fn send_msg_or_log(&self, method: &str, msg: Message) {
        if let Err(e) = self.tx.send(msg).await {
            warn!(
                "HotspotRecoder::{} fail to send \
                measurement to recoder, err:{}",
                method, e
            );
        }
    }
}
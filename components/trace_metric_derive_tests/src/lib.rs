// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

#[derive(Debug, Clone, TraceMetricWhenDrop)]
pub struct ExampleMetrics {
    #[metric(number, sum)]
    pub counter: usize,
    #[metric(duration)]
    pub elapsed: Duration,
    #[metric(boolean)]
    pub boolean: bool,
    pub foo: String,

    #[metric(collector)]
    pub collector: MetricsCollector,
}
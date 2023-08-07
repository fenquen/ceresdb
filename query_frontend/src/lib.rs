// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.
#![feature(once_cell_try)]
//! SQL frontend
//!
//! Parse sql into logical plan that can be handled by interpreters
#![allow(non_snake_case)]
pub mod ast;
pub mod container;
pub mod frontend;
pub mod influxql;
pub mod parser;
mod partition;
pub mod plan;
pub mod planner;
pub mod promql;
pub mod provider;

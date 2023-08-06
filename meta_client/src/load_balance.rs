// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Load balancer

use macros::define_result;
use rand::Rng;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Meta Addresses empty.\nBacktrace:\n{}", backtrace))]
    MetaAddressesEmpty { backtrace: Backtrace },
}

define_result!(Error);

pub trait LoadBalancer {
    fn select<'a>(&self, addresses: &'a [String]) -> Result<&'a String>;
}

pub struct RandomLoadBalancer;

impl LoadBalancer for RandomLoadBalancer {
    fn select<'a>(&self, addresses: &'a [String]) -> Result<&'a String> {
        if addresses.is_empty() {
            return MetaAddressesEmpty.fail();
        }

        let len = addresses.len();
        if len == 1 {
            return Ok(&addresses[0]);
        }
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0, len);

        Ok(&addresses[idx])
    }
}

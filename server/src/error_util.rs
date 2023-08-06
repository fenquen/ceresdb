// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Our backtrace is defined like this
// #[snafu(display("Time range is not found.\nBacktrace\n:{}", backtrace))]
//
// So here we split by `Backtrace`, and return first part
pub fn remove_backtrace_from_err(err_string: &str) -> &str {
    err_string
        .split("Backtrace")
        .next()
        .map(|s| s.trim_end())
        .unwrap_or(err_string)
}
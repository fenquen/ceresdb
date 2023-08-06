// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Toml config utilities.

use std::{fs::File, io::Read};

use macros::define_result;
use serde::de;
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to open file, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    OpenFile {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to read toml, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    ReadToml {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse toml, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    ParseToml {
        path: String,
        source: toml::de::Error,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Read toml file from given `path` to `toml_buf`, then parsed it to `T` and
/// return.
pub fn parse_toml_from_path<T>(path: &str, toml_buf: &mut String) -> Result<T>
where
    T: de::DeserializeOwned,
{
    let mut file = File::open(path).context(OpenFile { path })?;
    file.read_to_string(toml_buf).context(ReadToml { path })?;

    toml::from_str(toml_buf).context(ParseToml { path })
}
// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Copy from IOx
// https://github.com/influxdata/influxdb_iox/blob/d0f588d3b800894fe0ebd06b6f9a184ca6a603d7/predicate/src/regex.rs

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, StringArray, UInt64Array},
    datatypes::DataType,
};
use codec::{compact::MemCompactEncoder, Encoder};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{create_udf, Expr, Volatility},
    physical_plan::{functions::make_scalar_function, udf::ScalarUDF},
};
use hash_ext::hash64;

/// The name of the regex_match UDF given to DataFusion.
pub const REGEX_MATCH_UDF_NAME: &str = "RegexMatch";
pub const REGEX_NOT_MATCH_UDF_NAME: &str = "RegexNotMatch";

/// Given a column containing string values and a single regex pattern,
/// `regex_match_expr` determines which values satisfy the pattern and which do
/// not.
///
/// If `matches` is true then this expression will filter values that do not
/// satisfy the regex (equivalent to `col ~= /pattern/`). If `matches` is
/// `false` then the expression will filter values that *do* match the regex,
/// which is equivalent to `col !~ /pattern/`.
///
/// This UDF is designed to support the regex operator that can be pushed down
/// via the InfluxRPC API.
pub fn regex_match_expr(input: Expr, pattern: String, matches: bool) -> Expr {
    // N.B., this function does not utilise the Arrow regexp compute kernel because
    // in order to act as a filter it needs to return a boolean array of comparison
    // results, not an array of strings as the regex compute kernel does.
    let func = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1); // only works over a single column at a time.

        let input_arr = &args[0].as_any().downcast_ref::<StringArray>().unwrap();

        let pattern = regex::Regex::new(&pattern).map_err(|e| {
            DataFusionError::Internal(format!("error compiling regex pattern: {e}"))
        })?;

        let results = input_arr
            .iter()
            .map(|row| {
                // in arrow, any value can be null.
                // Here we decide to make our UDF to return null when either base or exponent is
                // null.
                row.map(|v| pattern.is_match(v) == matches)
            })
            .collect::<BooleanArray>();

        Ok(Arc::new(results) as ArrayRef)
    };

    // make_scalar_function is a helper to support accepting scalar values as
    // well as arrays.
    let func = make_scalar_function(func);

    let udf_name = if matches {
        REGEX_MATCH_UDF_NAME
    } else {
        REGEX_NOT_MATCH_UDF_NAME
    };

    let udf = create_udf(
        udf_name,
        vec![DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Stable,
        func,
    );

    udf.call(vec![input])
}

pub fn create_unique_id(input_len: usize) -> ScalarUDF {
    let func = move |args: &[ArrayRef]| {
        if args.is_empty() {
            let builder = UUIDBuilder::new();
            let tsid: UInt64Array = [Some(builder.finish())].iter().collect();
            return Ok(Arc::new(tsid) as ArrayRef);
        }
        let array_len = args[0].len();
        let inputs = args
            .iter()
            .map(|a| {
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("tag column not string".to_string()))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        let mut builders = Vec::new();
        builders.resize_with(array_len, UUIDBuilder::new);
        for array in &inputs {
            array
                .iter()
                .zip(builders.iter_mut())
                .for_each(|(v, builder)| {
                    builder.write(v);
                });
        }
        let results: UInt64Array = builders.into_iter().map(|b| Some(b.finish())).collect();
        Ok(Arc::new(results) as ArrayRef)
    };

    create_udf(
        "create_unique_id",
        vec![DataType::Utf8; input_len],
        Arc::new(DataType::UInt64),
        Volatility::Stable,
        make_scalar_function(func),
    )
}

struct UUIDBuilder {
    encoder: MemCompactEncoder,
    buf: Vec<u8>,
}

impl UUIDBuilder {
    fn new() -> Self {
        Self {
            encoder: MemCompactEncoder,
            buf: Vec::new(),
        }
    }

    fn write(&mut self, value: Option<&str>) {
        let value = value.unwrap_or("");
        self.encoder
            .encode(&mut self.buf, value.as_bytes())
            .unwrap(); // write mem is safe
    }

    fn finish(self) -> u64 {
        hash64(&self.buf)
    }
}
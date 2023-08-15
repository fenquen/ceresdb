// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module contains the types for InfluxDB.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest,
};
use common_types::{
    column_schema::ColumnSchema, datum::Datum, record_batch::RecordBatch, schema::RecordSchema,
    time::Timestamp,
};
use generic_error::BoxError;
use http::Method;
use influxdb_line_protocol::FieldValue;
use interpreters::interpreter::Output;
use query_frontend::influxql::planner::CERESDB_MEASUREMENT_COLUMN_NAME;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{Internal, InternalNoCause, Result};

/// Influxql write request compatible with influxdb 1.8
///
/// It's derived from 1.x write api described in doc of influxdb 1.8:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
#[derive(Debug)]
pub struct WriteRequest {
    /// Data formatted in line protocol
    pub lines: String,

    /// Details about `db`, `precision` can be saw in [WriteParams]
    // TODO: `db` should be made use of in later.
    pub db: String,
    pub precision: Precision,
}

impl WriteRequest {
    pub fn new(lines: Bytes, params: WriteParams) -> Self {
        let lines = String::from_utf8_lossy(&lines).to_string();

        let precision = params.precision.as_str().into();

        WriteRequest {
            lines,
            db: params.db,
            precision,
        }
    }
}

pub type WriteResponse = ();

/// Query string parameters for write api
///
/// It's derived from query string parameters of write described in
/// doc of influxdb 1.8:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-string-parameters-2
///
/// NOTE:
///     - `db` is not required and default to `public` in CeresDB.
///     - `precision`'s default value is `ms` but not `ns` in CeresDB.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct WriteParams {
    pub db: String,
    pub precision: String,
}

impl Default for WriteParams {
    fn default() -> Self {
        Self {
            db: "public".to_string(),
            precision: "ms".to_string(),
        }
    }
}

/// Influxql query request compatible with influxdb 1.8
///
/// It's derived from 1.x query api described in doc of influxdb 1.8:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint
///
/// NOTE:
///     - when query by POST method, query(q) should be placed after
///       `--data-urlencode` like what shown in link above.
///     - when query by GET method, query should be placed in url
///       parameters(where `db`, `epoch`, etc are placed in).
#[derive(Debug)]
pub struct InfluxqlRequest {
    /// Query described by influxql
    pub query: String,

    /// Details about `db`, `epoch`, `pretty` can be saw in [InfluxqlParams]
    // TODO: `db`, `epoch`, `pretty` should be made use of in later.
    pub db: String,
    pub epoch: Precision,
    pub pretty: bool,
}

impl InfluxqlRequest {
    pub fn try_new(
        method: Method,
        mut body: HashMap<String, String>,
        params: InfluxqlParams,
    ) -> Result<Self> {
        // Extract and check body & parameters.
        //  - q: required(in body when POST and parameters when GET)
        //  - chunked,db,epoch,pretty: in parameters
        if body.contains_key("params") {
            return InternalNoCause {
                msg: "`params` is not supported now",
            }
            .fail();
        }

        let query = match method {
            Method::GET => params.q.context(InternalNoCause {
                msg: "query not found when query by GET",
            })?,
            Method::POST => body.remove("q").context(InternalNoCause {
                msg: "query not found when query by POST",
            })?,
            other => {
                return InternalNoCause {
                    msg: format!("method not allowed in query, method:{other}"),
                }
                .fail()
            }
        };

        let epoch = params.epoch.as_str().into();

        Ok(InfluxqlRequest {
            query,
            db: params.db,
            epoch,
            pretty: params.pretty,
        })
    }
}

#[derive(Debug, Default)]
pub enum Precision {
    #[default]
    Millisecond,
    Nanosecond,
    Microsecond,
    Second,
    Minute,
    Hour,
}

impl Precision {
    fn try_normalize(&self, ts: i64) -> Option<i64> {
        match self {
            Self::Millisecond => Some(ts),
            Self::Nanosecond => ts.checked_div(1000 * 1000),
            Self::Microsecond => ts.checked_div(1000),
            Self::Second => ts.checked_mul(1000),
            Self::Minute => ts.checked_mul(1000 * 60),
            Self::Hour => ts.checked_mul(1000 * 60 * 60),
        }
    }
}

impl From<&str> for Precision {
    fn from(value: &str) -> Self {
        match value {
            "ns" | "n" => Precision::Nanosecond,
            "u" | "µ" => Precision::Microsecond,
            "ms" => Precision::Millisecond,
            "s" => Precision::Second,
            "m" => Precision::Minute,
            "h" => Precision::Hour,
            // Return the default precision.
            _ => Precision::Millisecond,
        }
    }
}

/// Query string parameters for query api(by influxql)
///
/// It's derived from query string parameters of query described in
/// doc of influxdb 1.8:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-string-parameters-1
///
/// NOTE:
///     - `db` is not required and default to `public` in CeresDB.
///     - `chunked` is not supported in CeresDB.
///     - `epoch`'s default value is `ms` but not `ns` in CeresDB.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct InfluxqlParams {
    pub q: Option<String>,
    pub db: String,
    pub epoch: String,
    pub pretty: bool,
}

impl Default for InfluxqlParams {
    fn default() -> Self {
        Self {
            q: None,
            db: "public".to_string(),
            epoch: "ms".to_string(),
            pretty: false,
        }
    }
}

/// Influxql response organized in the same way with influxdb.
///
/// The basic example:
/// ```json
/// {"results":[{"statement_id":0,"series":[{"name":"mymeas",
///                                          "columns":["time","myfield","mytag1","mytag2"],
///                                          "values":[["2017-03-01T00:16:18Z",33.1,null,null],
///                                                    ["2017-03-01T00:17:18Z",12.4,"12","14"]]}]}]}
/// ```
/// More details refer to:
///   https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-data-with-a-select-statement
#[derive(Debug, Serialize)]
pub struct InfluxqlResponse {
    pub results: Vec<OneInfluxqlResult>,
}

#[derive(Debug, Serialize)]
pub struct OneInfluxqlResult {
    statement_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    series: Option<Vec<OneSeries>>,
}

#[derive(Debug, Serialize)]
struct OneSeries {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<BTreeMap<String, String>>,
    columns: Vec<String>,
    values: Vec<Vec<Datum>>,
}

/// [InfluxqlResult] builder
#[derive(Default)]
pub struct InfluxqlResultBuilder {
    /// Query id for [multiple queries](https://docs.influxdata.com/influxdb/v1.8/tools/api/#request-multiple-queries)
    statement_id: u32,

    /// Schema of influxql query result
    ///
    /// Its format is like:
    /// measurement |
    /// tag_1..tag_n(columns in `group by`) |
    /// time |
    /// column_1..column_n(column in `projection` but not in `group by`)
    column_schemas: Vec<ColumnSchema>,

    /// Tags part in schema
    group_by_tag_col_idxs: Vec<usize>,

    /// Columns part in schema(include `time`)
    value_col_idxs: Vec<usize>,

    /// Mapping group key(`measurement` + `tag values`) to column values,
    ///
    /// NOTE: because tag keys in `group by` clause are same in each sub result,
    /// we just use the `measurement` + `tag values` to distinguish them.
    group_key_to_idx: HashMap<GroupKey, usize>,

    /// Column values grouped by [GroupKey]
    value_groups: Vec<RowGroup>,
}

type Row = Vec<Datum>;
type RowGroup = Vec<Row>;

impl InfluxqlResultBuilder {
    pub fn new(record_schema: &RecordSchema, statement_id: u32) -> Result<Self> {
        let column_schemas = record_schema.columns().to_owned();
        ensure!(
            !column_schemas.is_empty(),
            InternalNoCause {
                msg: "empty schema",
            }
        );

        // Query like `show measurements`, there will be not timestamp.
        let has_timestamp = column_schemas.iter().any(|c| c.datumKind.is_timestamp());
        // Find the tags part and columns part from schema.
        let mut group_by_col_idxs = Vec::new();
        let mut value_col_idxs = Vec::new();

        // The following index searching logic is derived from the fixed format
        // described when introducing `column_schemas`.
        let mut col_iter = column_schemas.iter().enumerate();
        // The first column may be measurement column in normal.
        ensure!(col_iter.next().unwrap().1.name == CERESDB_MEASUREMENT_COLUMN_NAME, InternalNoCause {
            msg: format!("invalid schema whose first column is not measurement column, schema:{column_schemas:?}"),
        });

        // The group by tags will be placed after measurement and before time column.
        let mut searching_group_by_tags = has_timestamp;
        for (idx, col) in col_iter {
            if col.datumKind.is_timestamp() {
                searching_group_by_tags = false;
            }

            if searching_group_by_tags {
                group_by_col_idxs.push(idx);
            } else {
                value_col_idxs.push(idx);
            }
        }

        Ok(Self {
            statement_id,
            column_schemas,
            group_by_tag_col_idxs: group_by_col_idxs,
            value_col_idxs,
            group_key_to_idx: HashMap::new(),
            value_groups: Vec::new(),
        })
    }

    pub fn add_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        // Check schema's compatibility.
        ensure!(
            record_batch.schema().columns() == self.column_schemas,
            InternalNoCause {
                msg: format!(
                    "conflict schema, origin:{:?}, new:{:?}",
                    self.column_schemas,
                    record_batch.schema().columns()
                ),
            }
        );

        let row_num = record_batch.num_rows();
        for row_idx in 0..row_num {
            // Get measurement + group by tags.
            let group_key = self.extract_group_key(&record_batch, row_idx)?;
            let value_group = self.extract_value_group(&record_batch, row_idx)?;

            let value_groups = if let Some(idx) = self.group_key_to_idx.get(&group_key) {
                self.value_groups.get_mut(*idx).unwrap()
            } else {
                self.value_groups.push(Vec::new());
                self.group_key_to_idx
                    .insert(group_key, self.value_groups.len() - 1);
                self.value_groups.last_mut().unwrap()
            };

            value_groups.push(value_group);
        }

        Ok(())
    }

    pub fn build(self) -> OneInfluxqlResult {
        let ordered_group_keys = {
            let mut ordered_pairs = self.group_key_to_idx.into_iter().collect::<Vec<_>>();
            ordered_pairs.sort_by(|a, b| a.1.cmp(&b.1));
            ordered_pairs
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>()
        };

        let series = ordered_group_keys
            .into_iter()
            .zip(self.value_groups.into_iter())
            .map(|(group_key, value_group)| {
                let name = group_key.measurement;
                let tags = if group_key.group_by_tag_values.is_empty() {
                    None
                } else {
                    let tags = group_key
                        .group_by_tag_values
                        .into_iter()
                        .enumerate()
                        .map(|(tagk_idx, tagv)| {
                            let tagk_col_idx = self.group_by_tag_col_idxs[tagk_idx];
                            let tagk = self.column_schemas[tagk_col_idx].name.clone();

                            (tagk, tagv)
                        })
                        .collect::<BTreeMap<_, _>>();

                    Some(tags)
                };

                let columns = self
                    .value_col_idxs
                    .iter()
                    .map(|idx| self.column_schemas[*idx].name.clone())
                    .collect::<Vec<_>>();

                OneSeries {
                    name,
                    tags,
                    columns,
                    values: value_group,
                }
            })
            .collect();

        OneInfluxqlResult {
            series: Some(series),
            statement_id: self.statement_id,
        }
    }

    fn extract_group_key(&self, record_batch: &RecordBatch, row_idx: usize) -> Result<GroupKey> {
        let mut group_by_tag_values = Vec::with_capacity(self.group_by_tag_col_idxs.len());
        let measurement = {
            let measurement = record_batch.column(0).datum(row_idx);
            match measurement {
                Datum::String(m) => m.to_string(),
                other => {
                    return InternalNoCause {
                        msg: format!("invalid measurement column, column:{other:?}"),
                    }
                    .fail()
                }
            }
        };

        for col_idx in &self.group_by_tag_col_idxs {
            let tag = {
                let tag_datum = record_batch.column(*col_idx).datum(row_idx);
                match tag_datum {
                    Datum::Null => "".to_string(),
                    Datum::String(tag) => tag.to_string(),
                    other => {
                        return InternalNoCause {
                            msg: format!("invalid tag column, column:{other:?}"),
                        }
                        .fail()
                    }
                }
            };
            group_by_tag_values.push(tag);
        }

        Ok(GroupKey {
            measurement,
            group_by_tag_values,
        })
    }

    fn extract_value_group(
        &self,
        record_batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Vec<Datum>> {
        let mut value_group = Vec::with_capacity(self.value_col_idxs.len());
        for col_idx in &self.value_col_idxs {
            let value = record_batch.column(*col_idx).datum(row_idx);

            value_group.push(value);
        }

        Ok(value_group)
    }
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct GroupKey {
    measurement: String,
    group_by_tag_values: Vec<String>,
}

pub(crate) fn convert_write_request(req: WriteRequest) -> Result<Vec<WriteTableRequest>> {
    let mut req_by_measurement = HashMap::new();
    for line in influxdb_line_protocol::parse_lines(&req.lines) {
        let mut line = line.box_err().with_context(|| Internal {
            msg: "invalid line",
        })?;

        let timestamp = match line.timestamp {
            Some(ts) => req.precision.try_normalize(ts).context(InternalNoCause {
                msg: "time outside range -9223372036854775806 - 9223372036854775806",
            })?,
            None => Timestamp::now().as_i64(),
        };
        let mut tag_set = line.series.tag_set.unwrap_or_default();
        // sort by tag key
        tag_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        // sort by field key
        line.field_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let tag_names = tag_set
            .iter()
            .map(|(tagk, _)| tagk.to_string())
            .collect::<Vec<_>>();
        let req_for_one_measurement = req_by_measurement
            // Fields with same tag names will be grouped together.
            .entry((line.series.measurement.to_string(), tag_names.clone()))
            .or_insert_with(|| WriteTableRequest {
                table: line.series.measurement.to_string(),
                tag_names,
                field_names: line
                    .field_set
                    .iter()
                    .map(|(tagk, _)| tagk.to_string())
                    .collect(),
                entries: Vec::new(),
            });

        let tags: Vec<_> = tag_set
            .iter()
            .enumerate()
            .map(|(idx, (_, tagv))| Tag {
                name_index: idx as u32,
                value: Some(Value {
                    value: Some(value::Value::StringValue(tagv.to_string())),
                }),
            })
            .collect();
        let field_group = FieldGroup {
            timestamp,
            fields: line
                .field_set
                .iter()
                .cloned()
                .enumerate()
                .map(|(idx, (_, fieldv))| Field {
                    name_index: idx as u32,
                    value: Some(convert_influx_value(fieldv)),
                })
                .collect(),
        };
        let mut found = false;
        for entry in &mut req_for_one_measurement.entries {
            if entry.tags == tags {
                // TODO: remove clone?
                entry.field_groups.push(field_group.clone());
                found = true;
                break;
            }
        }
        if !found {
            req_for_one_measurement.entries.push(WriteSeriesEntry {
                tags,
                field_groups: vec![field_group],
            })
        }
    }

    Ok(req_by_measurement.into_values().collect())
}

/// Convert influxdb's FieldValue to ceresdbproto's Value
fn convert_influx_value(field_value: FieldValue) -> Value {
    let v = match field_value {
        FieldValue::I64(v) => value::Value::Int64Value(v),
        FieldValue::U64(v) => value::Value::Uint64Value(v),
        FieldValue::F64(v) => value::Value::Float64Value(v),
        FieldValue::String(v) => value::Value::StringValue(v.to_string()),
        FieldValue::Boolean(v) => value::Value::BoolValue(v),
    };

    Value { value: Some(v) }
}

pub(crate) fn convert_influxql_output(output: Output) -> Result<InfluxqlResponse> {
    // TODO: now, we just support one influxql in each query.
    let records = match output {
        Output::Records(records) => records,
        Output::AffectedRows(_) => {
            return InternalNoCause {
                msg: "output in influxql should not be affected rows",
            }
            .fail()
        }
    };

    let influxql_result = if records.is_empty() {
        OneInfluxqlResult {
            statement_id: 0,
            series: None,
        }
    } else {
        // All record schemas in one query result should be same.
        let record_schema = records.first().unwrap().schema();
        let mut builder = InfluxqlResultBuilder::new(record_schema, 0)?;
        for record in records {
            builder.add_record_batch(record)?;
        }

        builder.build()
    };

    Ok(InfluxqlResponse {
        results: vec![influxql_result],
    })
}
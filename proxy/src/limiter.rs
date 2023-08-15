// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, sync::RwLock};

use datafusion::logical_expr::logical_plan::LogicalPlan;
use macros::define_result;
use query_frontend::plan::Plan;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Queried table is blocked, table:{}.\nBacktrace:\n{}",
        table,
        backtrace
    ))]
    BlockedTable { table: String, backtrace: Backtrace },

    #[snafu(display("Query is blocked by rule:{:?}.\nBacktrace:\n{}", rule, backtrace))]
    BlockedByRule {
        rule: BlockRule,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[derive(Clone, Copy, Deserialize, Debug, PartialEq, Eq, Hash, Serialize, PartialOrd, Ord)]
pub enum BlockRule {
    QueryWithoutPredicate,
    AnyQuery,
    AnyInsert,
}

#[derive(Default, Clone, Deserialize, Debug, Serialize)]
#[serde(default)]
pub struct LimiterConfig {
    pub write_block_list: Vec<String>,
    pub read_block_list: Vec<String>,
    pub rules: Vec<BlockRule>,
}

impl BlockRule {
    fn should_limit(&self, plan: &Plan) -> bool {
        match self {
            BlockRule::QueryWithoutPredicate => self.is_query_without_predicate(plan),
            BlockRule::AnyQuery => matches!(plan, Plan::Query(_)),
            BlockRule::AnyInsert => matches!(plan, Plan::Insert(_)),
        }
    }

    fn is_query_without_predicate(&self, plan: &Plan) -> bool {
        if let Plan::Query(query) = plan {
            !Self::contains_filter(&query.dataFusionLogicalPlan)
        } else {
            false
        }
    }

    fn contains_filter(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Filter(filter) = plan {
            return matches!(&*filter.input, LogicalPlan::TableScan(_));
        }

        for input in plan.inputs() {
            if Self::contains_filter(input) {
                return true;
            }
        }

        false
    }
}

pub struct Limiter {
    write_block_list: RwLock<HashSet<String>>,
    read_block_list: RwLock<HashSet<String>>,
    rules: RwLock<HashSet<BlockRule>>,
}

impl Default for Limiter {
    fn default() -> Self {
        Self {
            write_block_list: RwLock::new(HashSet::new()),
            read_block_list: RwLock::new(HashSet::new()),
            rules: RwLock::new(HashSet::new()),
        }
    }
}

impl Limiter {
    pub fn new(limit_config: LimiterConfig) -> Self {
        Self {
            write_block_list: RwLock::new(limit_config.write_block_list.into_iter().collect()),
            read_block_list: RwLock::new(limit_config.read_block_list.into_iter().collect()),
            rules: RwLock::new(limit_config.rules.into_iter().collect()),
        }
    }

    fn try_limit_by_block_list(&self, plan: &Plan) -> Result<()> {
        match plan {
            Plan::Query(query) => {
                self.read_block_list
                    .read()
                    .unwrap()
                    .iter()
                    .try_for_each(|blocked_table| {
                        if query
                            .tableContainer
                            .get(query_frontend::planner::get_table_ref(blocked_table))
                            .is_some()
                        {
                            BlockedTable {
                                table: blocked_table,
                            }
                            .fail()?;
                        }

                        Ok(())
                    })?;
            }
            Plan::Insert(insert) => {
                if self
                    .write_block_list
                    .read()
                    .unwrap()
                    .contains(insert.table.name())
                {
                    BlockedTable {
                        table: insert.table.name(),
                    }
                    .fail()?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn try_limit_by_rules(&self, plan: &Plan) -> Result<()> {
        self.rules.read().unwrap().iter().try_for_each(|rule| {
            if rule.should_limit(plan) {
                BlockedByRule { rule: *rule }.fail()?;
            }

            Ok(())
        })
    }

    /// Try to limit the plan according the configured limiter.
    ///
    /// Error will throws if the plan is forbidden to execute.
    pub fn tryLimit(&self, plan: &Plan) -> Result<()> {
        self.try_limit_by_block_list(plan)?;
        self.try_limit_by_rules(plan)
    }

    pub fn add_write_block_list(&self, block_list: Vec<String>) {
        self.write_block_list
            .write()
            .unwrap()
            .extend(block_list.into_iter())
    }

    pub fn add_read_block_list(&self, block_list: Vec<String>) {
        self.read_block_list
            .write()
            .unwrap()
            .extend(block_list.into_iter())
    }

    pub fn set_write_block_list(&self, block_list: Vec<String>) {
        *self.write_block_list.write().unwrap() = block_list.into_iter().collect();
    }

    pub fn set_read_block_list(&self, block_list: Vec<String>) {
        *self.read_block_list.write().unwrap() = block_list.into_iter().collect();
    }

    pub fn get_write_block_list(&self) -> HashSet<String> {
        self.write_block_list.read().unwrap().clone()
    }

    pub fn get_read_block_list(&self) -> HashSet<String> {
        self.read_block_list.read().unwrap().clone()
    }

    pub fn remove_write_block_list(&self, block_list: Vec<String>) {
        let mut write_block_list = self.write_block_list.write().unwrap();
        for value in block_list {
            write_block_list.remove(&value);
        }
    }

    pub fn remove_read_block_list(&self, block_list: Vec<String>) {
        let mut read_block_list = self.read_block_list.write().unwrap();
        for value in block_list {
            read_block_list.remove(&value);
        }
    }

    pub fn get_block_rules(&self) -> HashSet<BlockRule> {
        self.rules.read().unwrap().clone()
    }

    pub fn add_block_rules(&self, rules: Vec<BlockRule>) {
        self.rules.write().unwrap().extend(rules.into_iter());
    }

    pub fn remove_block_rules(&self, rules_to_remove: &[BlockRule]) {
        let mut rules = self.rules.write().unwrap();

        for rule_to_remove in rules_to_remove {
            rules.remove(rule_to_remove);
        }
    }

    pub fn set_block_rules(&self, new_rules: Vec<BlockRule>) {
        let new_rule_set: HashSet<_> = new_rules.into_iter().collect();
        *self.rules.write().unwrap() = new_rule_set;
    }
}
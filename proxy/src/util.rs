// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::prometheus::{
    expr::{Node, Node::Operand},
    operand::Value::Selector,
    sub_expr::OperatorType,
    Expr, SubExpr,
};
use table_engine::partition::{format_sub_partition_table_name, PartitionInfo};

pub fn get_sub_partition_name(
    table_name: &str,
    partition_info: &PartitionInfo,
    id: usize,
) -> String {
    let partition_name = partition_info.get_definitions()[id].name.clone();
    format_sub_partition_table_name(table_name, &partition_name)
}

fn table_from_sub_expr(expr: &SubExpr) -> Option<String> {
    if expr.op_type == OperatorType::Aggr as i32 || expr.op_type == OperatorType::Func as i32 {
        return table_from_expr(&expr.operands[0]);
    }

    None
}

pub fn table_from_expr(expr: &Expr) -> Option<String> {
    if let Some(node) = &expr.node {
        match node {
            Operand(operand) => {
                if let Some(op_value) = &operand.value {
                    match op_value {
                        Selector(sel) => return Some(sel.measurement.to_string()),
                        _ => return None,
                    }
                }
            }
            Node::SubExpr(sub_expr) => return table_from_sub_expr(sub_expr),
        }
    }

    None
}
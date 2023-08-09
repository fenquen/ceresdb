// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL parser
//!
//! Some codes are copied from datafusion: <https://github.com/apache/arrow/blob/9d86440946b8b07e03abb94fad2da278affae08f/rust/datafusion/src/sql/parser.rs#L74>

use log::debug;
use macros::define_result;
use paste::paste;
use sqlparser::{
    ast::{
        ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Ident, ObjectName, SetExpr,
        Statement as SqlStatement, TableConstraint, TableFactor, TableWithJoins,
    },
    dialect::{keywords::Keyword, Dialect, MySqlDialect},
    parser::{IsOptional::Mandatory, Parser as SqlParser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use table_engine::ANALYTIC_ENGINE_TYPE;

use crate::{
    ast::{
        AlterAddColumn, AlterModifySetting, CreateTable, DescribeTable, DropTable, ExistsTable,
        HashPartition, KeyPartition, Partition, ShowCreate, ShowCreateObject, ShowTables,
        Statement,
    },
    partition,
};

define_result!(ParserError);

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG))
    };
}

const TS_KEY: &str = "__ts_key";
const TAG: &str = "TAG";
const DICTIONARY: &str = "DICTIONARY";
const UNSIGN: &str = "UNSIGN";
const MODIFY: &str = "MODIFY";
const SETTING: &str = "SETTING";

macro_rules! is_custom_column {
    ($name: ident) => {
        paste! {
            #[inline]
            pub  fn [<is_ $name:lower _column>](opt: &ColumnOption) -> bool {
                match opt {
                    ColumnOption::DialectSpecific(tokens) => {
                        if let [Token::Word(word)] = &tokens[..] {
                            return word.value == $name;
                        }
                    }
                    _ => return false,
                }
                return false;
            }

        }
    };
}

is_custom_column!(TAG);
is_custom_column!(DICTIONARY);
is_custom_column!(UNSIGN);

/// Get the comment from the [`ColumnOption`] if it is a comment option.
#[inline]
pub fn get_column_comment(opt: &ColumnOption) -> Option<String> {
    if let ColumnOption::Comment(comment) = opt {
        return Some(comment.clone());
    }

    None
}

/// Get the default value expr from  [`ColumnOption`] if it is a default-value
/// option.
pub fn get_default_value(opt: &ColumnOption) -> Option<Expr> {
    if let ColumnOption::Default(expr) = opt {
        return Some(expr.clone());
    }

    None
}

/// Returns true when is a TIMESTAMP KEY table constraint
pub fn is_timestamp_key_constraint(constraint: &TableConstraint) -> bool {
    if let TableConstraint::Unique {
        name: Some(Ident {
                       value,
                       quote_style: None,
                   }),
        columns: _,
        is_primary: false,
    } = constraint
    {
        return value == TS_KEY;
    }
    false
}

/// 内部的包裹了dfSqlParser狐假虎威
pub struct Parser<'a> {
    dfSqlParser: SqlParser<'a>,
}

impl<'a> Parser<'a> {
    // Parse the specified tokens with dialect
    fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(Parser {
            dfSqlParser: SqlParser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
        // Use MySqlDialect, so we can support "`" and chinese characters.
        let dialect = &MySqlDialect {};
        let mut parser = Parser::new_with_dialect(sql, dialect)?;

        let mut statementVec = Vec::new();

        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.dfSqlParser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.dfSqlParser.peek_token() == Token::EOF {
                break;
            }

            // 说明之前已经读到东西了应该立即eof 然而现在却不是 已经了过
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.dfSqlParser.peek_token().token);
            }

            let statement = parser.parse_statement()?;
            statementVec.push(statement);
            expecting_statement_delimiter = true;
        }

        debug!("Parser parsed sql, sql:{}, stmts:{:#?}", sql, statementVec);

        Ok(statementVec)
    }

    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    // Parse a new expression
    fn parse_statement(&mut self) -> Result<Statement> {
        match self.dfSqlParser.peek_token().token {
            // key word
            Token::Word(word) => {
                match word.keyword {
                    Keyword::CREATE => {
                        // Move one token forward
                        self.dfSqlParser.next_token();
                        // Use custom parse
                        self.parse_create()
                    }
                    Keyword::DROP => {
                        // Move one token forward
                        self.dfSqlParser.next_token();
                        // Use custom parse
                        self.parse_drop()
                    }
                    Keyword::DESCRIBE | Keyword::DESC => {
                        self.dfSqlParser.next_token();
                        self.parse_describe()
                    }
                    Keyword::ALTER => {
                        self.dfSqlParser.next_token();
                        self.parse_alter()
                    }
                    Keyword::SHOW => {
                        self.dfSqlParser.next_token();
                        self.parse_show()
                    }
                    Keyword::EXISTS => {
                        self.dfSqlParser.next_token();
                        self.parse_exists()
                    }
                    _ => { // use the original parser
                        let mut statement = self.dfSqlParser.parse_statement()?;
                        maybe_normalize_table_name(&mut statement);
                        Ok(Statement::Standard(Box::new(statement)))
                    }
                }
            }
            _ => { // use the original parser
                Ok(Statement::Standard(Box::new(self.dfSqlParser.parse_statement()?)))
            }
        }
    }

    pub fn parse_alter(&mut self) -> Result<Statement> {
        let nth1_token = self.dfSqlParser.peek_token().token;
        let nth2_token = self.dfSqlParser.peek_nth_token(2).token;
        let nth3_token = self.dfSqlParser.peek_nth_token(3).token;
        if let (Token::Word(nth1_word), Token::Word(nth2_word), Token::Word(nth3_word)) =
            (nth1_token, nth2_token, nth3_token)
        {
            // example: ALTER TABLE test_ttl modify SETTING ttl='8d'
            if let (Keyword::TABLE, MODIFY, SETTING) = (
                nth1_word.keyword,
                nth2_word.value.to_uppercase().as_str(),
                nth3_word.value.to_uppercase().as_str(),
            ) {
                return self.parse_alter_modify_setting();
            }
            // examples:
            // ALTER TABLE test_table ADD COLUMN col_17 STRING TAG
            // ALTER TABLE test_table ADD COLUMN (col_18 STRING TAG, col_19 UNIT64)
            if let (Keyword::TABLE, Keyword::ADD, Keyword::COLUMN) =
                (nth1_word.keyword, nth2_word.keyword, nth3_word.keyword)
            {
                return self.parse_alter_add_column();
            }
        }
        Ok(Statement::Standard(Box::new(self.dfSqlParser.parse_alter()?)))
    }

    pub fn parse_show(&mut self) -> Result<Statement> {
        if self.consume_token("TABLES") {
            Ok(self.parse_show_tables()?)
        } else if self.consume_token("DATABASES") {
            Ok(Statement::ShowDatabases)
        } else if self.consume_token("CREATE") {
            Ok(self.parse_show_create()?)
        } else {
            self.expected("create/tables/databases", self.dfSqlParser.peek_token().token)
        }
    }

    fn parse_show_tables(&mut self) -> Result<Statement> {
        let pattern = match self.dfSqlParser.next_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Some(self.dfSqlParser.parse_literal_string()?),
                _ => return self.expected("like", self.dfSqlParser.peek_token().token),
            },
            Token::SemiColon | Token::EOF => None,
            _ => return self.expected(";", self.dfSqlParser.peek_token().token),
        };
        Ok(Statement::ShowTables(ShowTables { pattern }))
    }

    fn parse_show_create(&mut self) -> Result<Statement> {
        let obj_type = match self.dfSqlParser.expect_one_of_keywords(&[Keyword::TABLE])? {
            Keyword::TABLE => Ok(ShowCreateObject::Table),
            keyword => parser_err!(format!(
                "Unable to map keyword to ShowCreateObject: {keyword:?}"
            )),
        }?;

        let table_name = self.dfSqlParser.parse_object_name()?.into();

        Ok(Statement::ShowCreate(ShowCreate {
            obj_type,
            table_name,
        }))
    }

    fn parse_alter_add_column(&mut self) -> Result<Statement> {
        self.dfSqlParser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.dfSqlParser.parse_object_name()?.into();
        self.dfSqlParser
            .expect_keywords(&[Keyword::ADD, Keyword::COLUMN])?;
        let (mut columns, _) = self.parse_columns()?;
        if columns.is_empty() {
            let column_def = self.parse_column_def()?;
            columns.push(column_def);
        }
        Ok(Statement::AlterAddColumn(AlterAddColumn {
            table_name,
            columns,
        }))
    }

    fn parse_alter_modify_setting(&mut self) -> Result<Statement> {
        self.dfSqlParser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.dfSqlParser.parse_object_name()?.into();
        if self.consume_token(MODIFY) && self.consume_token(SETTING) {
            let options = self
                .dfSqlParser
                .parse_comma_separated(SqlParser::parse_sql_option)?;
            Ok(Statement::AlterModifySetting(AlterModifySetting {
                table_name,
                options,
            }))
        } else {
            unreachable!()
        }
    }

    pub fn parse_describe(&mut self) -> Result<Statement> {
        let _ = self.dfSqlParser.parse_keyword(Keyword::TABLE);
        let table_name = self.dfSqlParser.parse_object_name()?.into();
        Ok(Statement::Describe(DescribeTable { table_name }))
    }

    // Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement> {
        // Word包含了Keyword
        self.dfSqlParser.expect_keyword(Keyword::TABLE)?;

        let if_not_exists = self.dfSqlParser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let table_name = self.dfSqlParser.parse_object_name()?.into();

        let (columns, constraints) = self.parse_columns()?;

        // PARTITION BY...
        let partition = self.maybe_parse_and_check_partition(Keyword::PARTITION, &columns)?;

        // ENGINE = ...
        let engineName = self.parse_table_engine()?;

        // WITH ...
        let options = self.dfSqlParser.parse_options(Keyword::WITH)?;

        // Only String Column Can Be Dictionary Encoded
        for c in &columns {
            let mut is_dictionary = false;
            for op in &c.options {
                if is_dictionary_column(&op.option) {
                    is_dictionary = true;
                }
            }
            if c.data_type != DataType::String && is_dictionary {
                return parser_err!(format!(
                    "Only string column can be dictionary encoded: {:?}",
                    c.to_string()
                ));
            }
        }

        Ok(Statement::Create(Box::new(CreateTable {
            if_not_exists,
            table_name,
            columns,
            engineName,
            constraints,
            options,
            partition,
        })))
    }

    pub fn parse_drop(&mut self) -> Result<Statement> {
        self.dfSqlParser.expect_keyword(Keyword::TABLE)?;
        let if_exists = self.dfSqlParser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.dfSqlParser.parse_object_name()?.into();
        let engine = self.parse_table_engine()?;

        Ok(Statement::Drop(DropTable {
            table_name,
            if_exists,
            engine,
        }))
    }

    pub fn parse_exists(&mut self) -> Result<Statement> {
        let _ = self.dfSqlParser.parse_keyword(Keyword::TABLE);
        let table_name = self.dfSqlParser.parse_object_name()?.into();
        Ok(Statement::Exists(ExistsTable { table_name }))
    }

    // Copy from sqlparser
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.dfSqlParser.consume_token(&Token::LParen) || self.dfSqlParser.consume_token(&Token::RParen) {
            return Ok((Vec::new(), constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.dfSqlParser.peek_token().token {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition",
                                     self.dfSqlParser.peek_token().token, );
            }

            let comma = self.dfSqlParser.consume_token(&Token::Comma);
            if self.dfSqlParser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition",
                                     self.dfSqlParser.peek_token().token,
                );
            }
        }

        build_timestamp_key_constraint(&columns, &mut constraints);

        Ok((columns, constraints))
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok(ANALYTIC_ENGINE_TYPE.to_string());
        }

        self.dfSqlParser.expect_token(&Token::Eq)?;

        match self.dfSqlParser.next_token().token {
            Token::Word(w) => Ok(w.value),
            unexpected => self.expected("engine is missing", unexpected),
        }
    }

    // Copy from sqlparser
    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.dfSqlParser.parse_identifier()?;
        let data_type = self.dfSqlParser.parse_data_type()?;
        let collation = if self.dfSqlParser.parse_keyword(Keyword::COLLATE) {
            Some(self.dfSqlParser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.dfSqlParser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.dfSqlParser.parse_identifier()?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.dfSqlParser.peek_token().token,
                    );
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    // Copy from sqlparser by boyan
    fn parse_optional_table_constraint(&mut self) -> Result<Option<TableConstraint>> {
        let name = if self.dfSqlParser.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.dfSqlParser.parse_identifier()?)
        } else {
            None
        };
        match self.dfSqlParser.next_token().token {
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                self.dfSqlParser.expect_keyword(Keyword::KEY)?;
                let columns = self
                    .dfSqlParser
                    .parse_parenthesized_column_list(Mandatory, false)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::TIMESTAMP => {
                self.dfSqlParser.expect_keyword(Keyword::KEY)?;
                let columns = self
                    .dfSqlParser
                    .parse_parenthesized_column_list(Mandatory, false)?;
                // TODO(boyan), TableConstraint doesn't support dialect right now
                // we use unique constraint as TIMESTAMP KEY constraint.
                Ok(Some(TableConstraint::Unique {
                    name: Some(Ident {
                        value: TS_KEY.to_owned(),
                        quote_style: None,
                    }),
                    columns,
                    is_primary: false,
                }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, TIMESTAMP", unexpected)
                } else {
                    self.dfSqlParser.prev_token();
                    Ok(None)
                }
            }
        }
    }

    // Copy from sqlparser  by boyan
    fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>> {
        if self.dfSqlParser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.dfSqlParser.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.dfSqlParser.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.dfSqlParser.parse_expr()?)))
        } else if self
            .dfSqlParser
            .parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
        {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self
            .dfSqlParser
            .parse_keywords(&[Keyword::TIMESTAMP, Keyword::KEY])
        {
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(TS_KEY),
            ])))
        } else if self.consume_token(TAG) {
            // Support TAG for ceresdb
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(TAG),
            ])))
        } else if self.consume_token(DICTIONARY) {
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(DICTIONARY),
            ])))
        } else if self.consume_token(UNSIGN) {
            // Support unsign for ceresdb
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(UNSIGN),
            ])))
        } else if self.dfSqlParser.parse_keyword(Keyword::COMMENT) {
            Ok(Some(ColumnOption::Comment(
                self.dfSqlParser.parse_literal_string()?,
            )))
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_and_check_partition(
        &mut self,
        keyword: Keyword,
        columns: &[ColumnDef],
    ) -> Result<Option<Partition>> {
        // PARTITION BY ...
        if !self.dfSqlParser.parse_keyword(keyword) {
            return Ok(None);
        }
        self.dfSqlParser.expect_keyword(Keyword::BY)?;

        // Parse partition strategy.
        self.parse_and_check_partition_strategy(columns)
    }

    fn parse_and_check_partition_strategy(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<Partition>> {
        if let Some(key) = self.maybe_parse_and_check_key_partition(columns)? {
            return Ok(Some(Partition::Key(key)));
        }
        if let Some(hash) = self.maybe_parse_and_check_hash_partition(columns)? {
            return Ok(Some(Partition::Hash(hash)));
        }

        Ok(None)
    }

    fn maybe_parse_and_check_hash_partition(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<HashPartition>> {
        // Parse first part: "PARTITION BY HASH(expr)".
        let linear = if self.consume_token("HASH") {
            false
        } else if self.consume_tokens(&["LINEAR", "HASH"]) {
            true
        } else {
            return Ok(None);
        };

        // TODO: support all valid exprs not only column expr.
        let expr = self.parse_and_check_expr_in_hash(columns)?;

        let partition_num = self.parse_partition_num()?;

        // Parse successfully.
        Ok(Some(HashPartition {
            linear,
            partition_num,
            expr,
        }))
    }

    fn maybe_parse_and_check_key_partition(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<KeyPartition>> {
        let linear = if self.consume_token("KEY") {
            false
        } else if self.consume_tokens(&["LINEAR", "KEY"]) {
            true
        } else {
            return Ok(None);
        };

        let key_columns = self
            .dfSqlParser
            .parse_parenthesized_column_list(Mandatory, false)
            .map_err(|e| {
                ParserError::ParserError(format!("Fail to parse partition key, err:{e}"))
            })?;

        // Ensure at least one column for partition key.
        if key_columns.is_empty() {
            return parser_err!(
                "except at least one partition key, default partition key is unsupported now"
                    .to_string()
            );
        }

        // Validate all columns composing partition key:
        //  - The column must exist;
        //  - The column must be a tag;
        for key_col in &key_columns {
            let col_def = match columns.iter().find(|c| c.name.value == key_col.value) {
                Some(v) => v,
                None => {
                    return parser_err!(format!(
                        "partition key contains non-existent column:{}",
                        key_col.value,
                    ));
                }
            };
            let tag_column = col_def.options.iter().any(|opt| is_tag_column(&opt.option));
            if !tag_column {
                return parser_err!(format!(
                    "partition key must be tag, key name:{:?}",
                    key_col.value
                ));
            }
        }

        let partition_num = self.parse_partition_num()?;
        let partition_key = key_columns.into_iter().map(|v| v.value).collect();

        // Parse successfully.
        Ok(Some(KeyPartition {
            linear,
            partition_num,
            partition_key,
        }))
    }

    // Parse second part: "PARTITIONS num" (if not set, num will use 1 as default).
    fn parse_partition_num(&mut self) -> Result<u64> {
        let partition_num = if self.dfSqlParser.parse_keyword(Keyword::PARTITIONS) {
            match self.dfSqlParser.parse_number_value()? {
                sqlparser::ast::Value::Number(v, _) => match v.parse::<u64>() {
                    Ok(v) => v,
                    Err(e) => {
                        return parser_err!(format!("invalid partition num, raw:{v}, err:{e}"));
                    }
                },
                v => return parser_err!(format!("expect partition number, found:{v}")),
            }
        } else {
            1
        };

        if partition_num > partition::MAX_PARTITION_NUM {
            parser_err!(format!(
                "partition num must be <= MAX_PARTITION_NUM, MAX_PARTITION_NUM:{}, set partition num:{}",
                partition::MAX_PARTITION_NUM, partition_num
            ))
        } else {
            Ok(partition_num)
        }
    }

    fn parse_and_check_expr_in_hash(&mut self, columns: &[ColumnDef]) -> Result<Expr> {
        let expr = self.dfSqlParser.parse_expr()?;
        if let Expr::Nested(inner) = expr {
            match inner.as_ref() {
                Expr::Identifier(id) => {
                    if check_column_expr_validity_in_hash(id, columns) {
                        Ok(*inner)
                    } else {
                        parser_err!(format!("Expect column(tag, type: int, tiny int, small int, big int), search by column name:{id}"))
                    }
                }
                other => parser_err!(
                    format!("Only column expr in hash partition now, example: HASH(column name), found:{other:?}")
                ),
            }
        } else {
            parser_err!(format!("Expect nested expr, found:{expr:?}"))
        }
    }

    fn consume_token(&mut self, expected: &str) -> bool {
        if self.dfSqlParser.peek_token().to_string().to_uppercase() == *expected.to_uppercase() {
            self.dfSqlParser.next_token();
            true
        } else {
            false
        }
    }

    fn consume_tokens(&mut self, expected_tokens: &[&str]) -> bool {
        for expected in expected_tokens {
            if !self.consume_token(expected) {
                return false;
            }
        }
        true
    }
}

// Valid column expr in hash should meet following conditions:
// 1. column must be a tag, tsid + timestamp can be seen as the combined unique
// key, and partition key must be the subset of it(for supporting overwritten
// insert mode). About the reason, maybe you can see: https://docs.pingcap.com/zh/tidb/stable/partitioned-table
//
// 2. column's datatype must be integer(int, tiny int, small int, big int ...).
//
// TODO: we should consider the situation: no tag column is set.
fn check_column_expr_validity_in_hash(column: &Ident, columns: &[ColumnDef]) -> bool {
    let valid_column = columns.iter().find(|col| {
        if col.name == *column {
            let is_integer = matches!(
                col.data_type,
                DataType::Int(_)
                    | DataType::TinyInt(_)
                    | DataType::SmallInt(_)
                    | DataType::BigInt(_)
                    | DataType::UnsignedInt(_)
                    | DataType::UnsignedTinyInt(_)
                    | DataType::UnsignedSmallInt(_)
                    | DataType::UnsignedBigInt(_)
            );
            let tag_column = col.options.iter().any(|opt| is_tag_column(&opt.option));
            is_integer && tag_column
        } else {
            false
        }
    });

    valid_column.is_some()
}

// Build the tskey constraint from the column definitions if any.
fn build_timestamp_key_constraint(col_defs: &[ColumnDef], constraints: &mut Vec<TableConstraint>) {
    for col_def in col_defs {
        for col in &col_def.options {
            if let ColumnOption::DialectSpecific(tokens) = &col.option {
                if let [Token::Word(token)] = &tokens[..] {
                    if token.value.eq(TS_KEY) {
                        let constraint = TableConstraint::Unique {
                            name: Some(Ident {
                                value: TS_KEY.to_owned(),
                                quote_style: None,
                            }),
                            columns: vec![col_def.name.clone()],
                            is_primary: false,
                        };
                        constraints.push(constraint);
                    }
                }
            };
        }
    }
}

/// Add quotes in table name (for example: convert table to `table`) 为什么使用单引号应为使用的是sql的dialect
///
/// It is used to process table name in `SELECT`, for preventing `datafusion`
/// converting the table name to lowercase, because `CeresDB` only support
/// case-sensitive in sql.
// TODO: maybe other items(such as: alias, column name) need to be normalized too.
pub fn maybe_normalize_table_name(statement: &mut SqlStatement) {
    if let SqlStatement::Query(query) = statement {
        if let SetExpr::Select(select) = query.body.as_mut() {
            select.from.iter_mut().for_each(maybe_convert_one_from);
        }
    }
}

fn maybe_convert_one_from(one_from: &mut TableWithJoins) {
    let TableWithJoins { relation, joins } = one_from;
    maybe_convert_relation(relation);
    joins.iter_mut().for_each(|join| {
        maybe_convert_relation(&mut join.relation);
    });
}

fn maybe_convert_relation(relation: &mut TableFactor) {
    if let TableFactor::Table { name, .. } = relation {
        maybe_convert_table_name(name);
    }
}

fn maybe_convert_table_name(object_name: &mut ObjectName) {
    object_name.0.iter_mut().for_each(|id| {
        if id.quote_style.is_none() {
            let _ = std::mem::replace(id, Ident::with_quote('`', id.value.clone()));
        }
    })
}
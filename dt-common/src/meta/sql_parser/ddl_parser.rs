use crate::error::Error;
use anyhow::bail;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::{map, not, opt, peek},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use regex::Regex;

use std::{
    borrow::Cow,
    str::{self},
};

use crate::meta::ddl_type::DdlType;

use super::keywords::keyword_c_to_e;
use super::keywords::keyword_e_to_i;
use super::keywords::keyword_i_to_o;
use super::keywords::keyword_o_to_s;
use super::keywords::keyword_s_to_z;
use super::{keywords::keyword_a_to_c, parsed_ddl::ParsedDdl};

pub struct DdlParser {}

impl DdlParser {
    pub fn parse(sql: &str) -> anyhow::Result<ParsedDdl> {
        let sql = Self::remove_comments(sql);
        let input = sql.trim().as_bytes();
        match sql_query(input) {
            Ok((remaining_input, mut ddl)) => {
                ddl.unparsed_part = String::from_utf8_lossy(remaining_input).to_string();
                Ok(ddl)
            }
            Err(_) => bail! {Error::Unexpected(format!("failed to parse sql: {}", sql))},
        }
    }

    fn remove_comments(sql: &str) -> Cow<str> {
        // "create /*some comments,*/table/*some comments*/ `aaa`.`bbb`"
        let regex = Regex::new(r"(/\*([^*]|\*+[^*/*])*\*+/)|(--[^\n]*\n)").unwrap();
        regex.replace_all(sql, "")
    }
}

/// parse ddl sql and return: (ddl_type, schema, table)
#[allow(clippy::type_complexity)]
fn sql_query(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    alt((
        map(create_database, |r| r),
        map(drop_database, |r| r),
        map(alter_database, |r| r),
        map(create_schema, |r| r),
        map(drop_schema, |r| r),
        map(alter_schema, |r| r),
        map(create_table, |r| r),
        map(drop_table, |r| r),
        map(alter_table, |r| r),
        map(truncate_table, |r| r),
        map(rename_table, |r| r),
    ))(i)
}

fn create_database(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_not_exists, database, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        opt(if_not_exists),
        sql_identifier,
        multispace0,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::CreateDatabase,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        if_not_exists: if_not_exists.is_some(),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn drop_database(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_exists, database, _)) = tuple((
        tag_no_case("drop"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        opt(if_exists),
        sql_identifier,
        multispace0,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::DropDatabase,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        if_exists: if_exists.is_some(),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn alter_database(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, database, _)) = tuple((
        tag_no_case("alter"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        sql_identifier,
        multispace1,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::AlterDatabase,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn create_schema(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_not_exists, database, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("schema"),
        multispace1,
        opt(if_not_exists),
        sql_identifier,
        multispace0,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::CreateSchema,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        if_not_exists: if_not_exists.is_some(),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn drop_schema(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_exists, database, _)) = tuple((
        tag_no_case("drop"),
        multispace1,
        tag_no_case("schema"),
        multispace1,
        opt(if_exists),
        sql_identifier,
        multispace0,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::DropSchema,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        if_exists: if_exists.is_some(),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn alter_schema(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, database, _)) = tuple((
        tag_no_case("alter"),
        multispace1,
        tag_no_case("schema"),
        multispace1,
        sql_identifier,
        multispace1,
    ))(i)?;

    let ddl = ParsedDdl {
        ddl_type: DdlType::AlterSchema,
        schema: Some(String::from_utf8_lossy(database).to_string()),
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

type SchemaTable = (Option<Vec<u8>>, Vec<u8>);

fn create_table(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_not_exists, table, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(if_not_exists),
        schema_table_reference,
        multispace0,
    ))(i)?;

    let (schema, tb) = parse_table(table);
    let ddl = ParsedDdl {
        ddl_type: DdlType::CreateTable,
        if_not_exists: if_not_exists.is_some(),
        schema,
        tb,
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn drop_table(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, if_exists, table, _)) = tuple((
        tag_no_case("drop"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(if_exists),
        schema_table_reference,
        multispace0,
    ))(i)?;

    let (schema, tb) = parse_table(table);
    let ddl = ParsedDdl {
        ddl_type: DdlType::DropTable,
        if_exists: if_exists.is_some(),
        schema,
        tb,
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn alter_table(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("alter"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        schema_table_reference,
        multispace1,
    ))(i)?;

    let (schema, tb) = parse_table(table);
    let ddl = ParsedDdl {
        ddl_type: DdlType::AlterTable,
        schema,
        tb,
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn truncate_table(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("truncate"),
        multispace1,
        opt(tag_no_case("table")),
        opt(multispace1),
        schema_table_reference,
        multispace0,
    ))(i)?;

    let (schema, tb) = parse_table(table);
    let ddl = ParsedDdl {
        ddl_type: DdlType::TruncateTable,
        schema,
        tb,
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn rename_table(i: &[u8]) -> IResult<&[u8], ParsedDdl> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("rename"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        schema_table_reference,
        multispace0,
    ))(i)?;

    let (schema, tb) = parse_table(table);
    let ddl = ParsedDdl {
        ddl_type: DdlType::RenameTable,
        schema,
        tb,
        ..Default::default()
    };
    Ok((remaining_input, ddl))
}

fn if_not_exists(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = tuple((
        tag_no_case("if"),
        multispace1,
        tag_no_case("not"),
        multispace1,
        tag_no_case("exists"),
        multispace1,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn if_exists(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = tuple((
        tag_no_case("if"),
        multispace1,
        tag_no_case("exists"),
        multispace1,
    ))(i)?;
    Ok((remaining_input, ()))
}

// Parse a reference to a named schema.table, with an optional alias
fn schema_table_reference(i: &[u8]) -> IResult<&[u8], SchemaTable> {
    map(
        tuple((
            opt(pair(sql_identifier, pair(multispace0, tag(".")))),
            multispace0,
            sql_identifier,
        )),
        |tup| {
            let name = tup.2.to_vec();
            let schema = tup.0.map(|(schema, _)| schema.to_vec());
            (schema, name)
        },
    )(i)
}

#[inline]
fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

#[inline]
fn is_escaped_sql_identifier_1(chr: u8) -> bool {
    chr != b'`'
}

#[inline]
fn is_escaped_sql_identifier_2(chr: u8) -> bool {
    chr != b'"'
}

fn sql_identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
        delimited(tag("`"), take_while1(is_escaped_sql_identifier_1), tag("`")),
        delimited(
            tag("\""),
            take_while1(is_escaped_sql_identifier_2),
            tag("\""),
        ),
    ))(i)
}

// Matches any SQL reserved keyword
fn sql_keyword(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        keyword_a_to_c,
        keyword_c_to_e,
        keyword_e_to_i,
        keyword_i_to_o,
        keyword_o_to_s,
        keyword_s_to_z,
    ))(i)
}

fn parse_table(table: (Option<Vec<u8>>, Vec<u8>)) -> (Option<String>, Option<String>) {
    let schema = table
        .0
        .map(|schema| String::from_utf8_lossy(&schema).to_string());
    let tb = Some(String::from_utf8_lossy(&table.1).to_string());
    (schema, tb)
}

#[cfg(test)]
mod test {

    use crate::config::config_enums::DbType;

    use super::*;

    #[test]
    fn test_create_table_with_schema() {
        let sqls = vec![
            // schema.table
            "create table aaa.bbb (id int)",
            // escapes
            "create table `aaa`.`bbb` (id int)",
            // spaces
            "  create   table  aaa . bbb   (id int)  ",
            // spaces + escapes
            "  create   table  `aaa` . `bbb`   (id int)  ",
            // if not exists
            "create table if  not  exists `aaa`.`bbb` (id int)",
            // comments
            "create /*some comments,*/table/*some comments*/ `aaa`.`bbb` (id int)",
            //  escapes + spaces + if not exists + comments
            "create /*some comments,*/table/*some comments*/ if  not  exists  `aaa` .  `bbb` (id int)  ",
        ];

        let expect_sqls = vec![
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `aaa`.`bbb` (id int)",
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = DbType::Mysql;
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i])
        }
    }

    #[test]
    fn test_create_table_with_schema_with_special_characters() {
        let sqls = vec![
            // mysql
            "CREATE TABLE IF NOT EXISTS `test_db_*.*`.bbb(id int);",
            "CREATE TABLE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`.`中文!@$#$%^&*&(_+)`(id int);",
            // pg
            r#"CREATE TABLE IF NOT EXISTS "test_db_*.*".bbb(id int);"#,
            r#"CREATE TABLE IF NOT EXISTS "中文.others*&^%$#@!+_)(&^%#"."中文!@$#$%^&*&(_+)"(id int);"#,
        ];

        let db_types = vec![DbType::Mysql, DbType::Mysql, DbType::Pg, DbType::Pg];
        let dbs = vec![
            "test_db_*.*",
            "中文.others*&^%$#@!+_)(&^%#",
            "test_db_*.*",
            "中文.others*&^%$#@!+_)(&^%#",
        ];
        let tbs = vec!["bbb", "中文!@$#$%^&*&(_+)", "bbb", "中文!@$#$%^&*&(_+)"];

        let expect_sqls = vec![
            "CREATE TABLE IF NOT EXISTS `test_db_*.*`.`bbb` (id int);",
            "CREATE TABLE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`.`中文!@$#$%^&*&(_+)` (id int);",
            r#"CREATE TABLE IF NOT EXISTS "test_db_*.*"."bbb" (id int);"#,
            r#"CREATE TABLE IF NOT EXISTS "中文.others*&^%$#@!+_)(&^%#"."中文!@$#$%^&*&(_+)" (id int);"#,
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = db_types[i].to_owned();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.schema, Some(dbs[i].to_string()));
            assert_eq!(r.tb, Some(tbs[i].to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_without_schema() {
        let sqls = vec![
            // schema.table
            "create table bbb (id int)",
            // escapes
            "create table `bbb` (id int)",
            // spaces
            "  create   table  bbb   (id int)  ",
            // spaces + escapes
            "  create   table   `bbb`   (id int)  ",
            // if not exists
            "create table if  not  exists `bbb` (id int)",
            // comments
            "create /*some comments,*/table/*some comments*/ `bbb` (id int)",
            //  escapes + spaces + if not exists + comments
            "create /*some comments,*/table/*some comments*/ if  not  exists    `bbb` (id int)  ",
        ];

        let expect_sqls = vec![
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `bbb` (id int)",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.schema, None);
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_table_with_schema() {
        let sqls = vec![
            // schema.table
            "drop table aaa.bbb",
            // escapes
            "drop table `aaa`.`bbb`",
            // spaces
            "  drop   table  aaa . bbb  ",
            // spaces + escapes
            "  drop   table  `aaa` . `bbb`  ",
            // if exists
            "drop table if  exists `aaa`.`bbb`",
            // comments
            "drop /*some comments,*/table/*some comments*/ `aaa`.`bbb`",
            //  escapes + spaces + if exists + comments
            "drop /*some comments,*/table/*some comments*/ if  exists  `aaa` .  `bbb`  ",
        ];

        let expect_sqls = vec![
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE IF EXISTS `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE IF EXISTS `aaa`.`bbb`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::DropTable);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_table_without_schema() {
        let sqls = vec![
            // schema.table
            "drop table bbb",
            // escapes
            "drop table `bbb`",
            // spaces
            "  drop   table   bbb  ",
            // spaces + escapes
            "  drop   table   `bbb`  ",
            // if exists
            "drop table if  exists `bbb`",
            // comments
            "drop /*some comments,*/table/*some comments*/ `bbb`",
            //  escapes + spaces + if exists + comments
            "drop /*some comments,*/table/*some comments*/ if  exists    `bbb`  ",
        ];

        let expect_sqls = vec![
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE IF EXISTS `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE IF EXISTS `bbb`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::DropTable);
            assert_eq!(r.schema, None);
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_with_schema() {
        let sqls = vec![
            // schema.table
            "alter table aaa.bbb add column value int",
            // escapes
            "alter table `aaa`.`bbb` add column value int",
            // spaces
            "  alter   table  aaa . bbb   add column value int",
            // spaces + escapes
            "  alter   table  `aaa` . `bbb`   add column value int",
            // if exists
            "alter table `aaa`.`bbb` add column value int",
            // comments
            "alter /*some comments,*/table/*some comments*/ `aaa`.`bbb` add column value int",
            //  escapes + spaces + if exists + comments
            "alter /*some comments,*/table/*some comments*/   `aaa` .  `bbb`   add column value int",
        ];

        let expect_sqls = vec![
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // escapes
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // spaces
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // spaces + escapes
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // if exists
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // comments
            "ALTER TABLE `aaa`.`bbb` add column value int",
            //  escapes + spaces + if exists + comments
            "ALTER TABLE `aaa`.`bbb` add column value int",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_without_schema() {
        let sqls = vec![
            // schema.table
            "alter table bbb add column value int",
            // escapes
            "alter table `bbb` add column value int",
            // spaces
            "  alter   table   bbb   add column value int",
            // spaces + escapes
            "  alter   table   `bbb`   add column value int",
            // comments
            "alter /*some comments,*/table/*some comments*/ `bbb` add column value int",
            //  escapes + spaces + comments
            "alter /*some comments,*/table/*some comments*/    `bbb`   add column value int",
        ];

        let expect_sqls = vec![
            // schema.table
            "ALTER TABLE `bbb` add column value int",
            // escapes
            "ALTER TABLE `bbb` add column value int",
            // spaces
            "ALTER TABLE `bbb` add column value int",
            // spaces + escapes
            "ALTER TABLE `bbb` add column value int",
            // comments
            "ALTER TABLE `bbb` add column value int",
            // escapes + spaces + comments
            "ALTER TABLE `bbb` add column value int",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.schema, None);
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_database() {
        let sqls = vec![
            "create database aaa",
            // escapes
            "create database `aaa`",
            // spaces
            "  create   database   aaa",
            // spaces + escapes
            "  create   database   `aaa`  ",
            // if exists
            "create database if  not  exists `aaa`",
            // comments
            "create /*some comments,*/database/*some comments*/ `aaa`",
            //  escapes + spaces + if exists + comments
            "create /*some comments,*/database/*some comments*/ if  not  exists    `aaa`  ",
        ];

        let expect_sqls = vec![
            "CREATE DATABASE `aaa`",
            // escapes
            "CREATE DATABASE `aaa`",
            // spaces
            "CREATE DATABASE `aaa`",
            // spaces + escapes
            "CREATE DATABASE `aaa`",
            // if exists
            "CREATE DATABASE IF NOT EXISTS `aaa`",
            // comments
            "CREATE DATABASE `aaa`",
            //  escapes + spaces + if exists + comments
            "CREATE DATABASE IF NOT EXISTS `aaa`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateDatabase);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_database_with_special_characters() {
        let sqls = vec![
            "CREATE DATABASE IF NOT EXISTS `test_db_*.*`;",
            "CREATE DATABASE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`;",
        ];
        let dbs = vec!["test_db_*.*", "中文.others*&^%$#@!+_)(&^%#"];

        let expect_sqls = vec![
            "CREATE DATABASE IF NOT EXISTS `test_db_*.*` ;",
            "CREATE DATABASE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#` ;",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateDatabase);
            assert_eq!(r.schema, Some(dbs[i].to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_database() {
        let sqls = vec![
            "drop database aaa",
            // escapes
            "drop database `aaa`",
            // spaces
            "  drop   database   aaa",
            // spaces + escapes
            "  drop   database   `aaa`  ",
            // if exists
            "drop database if  exists `aaa`",
            // comments
            "drop /*some comments,*/database/*some comments*/ `aaa`",
            //  escapes + spaces + if exists + comments
            "drop /*some comments,*/database/*some comments*/ if  exists    `aaa`  ",
        ];

        let expect_sqls = vec![
            "DROP DATABASE `aaa`",
            // escapes
            "DROP DATABASE `aaa`",
            // spaces
            "DROP DATABASE `aaa`",
            // spaces + escapes
            "DROP DATABASE `aaa`",
            // if exists
            "DROP DATABASE IF EXISTS `aaa`",
            // comments
            "DROP DATABASE `aaa`",
            //  escapes + spaces + if exists + comments
            "DROP DATABASE IF EXISTS `aaa`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::DropDatabase);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_database() {
        let sqls = vec![
            "alter database aaa CHARACTER SET utf8",
            // escapes
            "alter database `aaa` CHARACTER SET utf8",
            // spaces
            "  alter   database   aaa CHARACTER SET utf8",
            // spaces + escapes
            "  alter   database   `aaa`   CHARACTER SET utf8",
            // comments
            "alter /*some comments,*/database/*some comments*/ `aaa` CHARACTER SET utf8",
            //  escapes + spaces + comments
            "alter /*some comments,*/database/*some comments*/    `aaa`   CHARACTER SET utf8",
        ];

        let expect_sqls = vec![
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // escapes
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // spaces
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // spaces + escapes
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // comments
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // escapes + spaces + comments
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterDatabase);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_truncate_table_with_schema() {
        let sqls = vec![
            // schema.table
            "truncate table aaa.bbb",
            // escapes
            "truncate table `aaa`.`bbb`",
            // spaces
            "  truncate   table  aaa . bbb  ",
            // spaces + escapes
            "  truncate   table  `aaa` . `bbb`  ",
            // comments
            "truncate /*some comments,*/table/*some comments*/ `aaa`.`bbb`",
            //  escapes + spaces + comments
            "truncate /*some comments,*/table/*some comments*/   `aaa` .  `bbb`  ",
            // without keyword `table`
            "truncate `aaa`.`bbb`",
            "truncate /*some comments,*/table/*some comments*/ `aaa`.`bbb`",
        ];

        let expect_sqls = vec![
            // schema.table
            "TRUNCATE TABLE `aaa`.`bbb`",
            // escapes
            "TRUNCATE TABLE `aaa`.`bbb`",
            // spaces
            "TRUNCATE TABLE `aaa`.`bbb`",
            // spaces + escapes
            "TRUNCATE TABLE `aaa`.`bbb`",
            // comments
            "TRUNCATE TABLE `aaa`.`bbb`",
            //  escapes + spaces + comments
            "TRUNCATE TABLE `aaa`.`bbb`",
            // without keyword `table`
            "TRUNCATE TABLE `aaa`.`bbb`",
            "TRUNCATE TABLE `aaa`.`bbb`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_truncate_table_without_schema() {
        let sqls = vec![
            // schema.table
            "truncate table bbb",
            // escapes
            "truncate table `bbb`",
            // spaces
            "  truncate   table   bbb  ",
            // spaces + escapes
            "  truncate   table   `bbb`  ",
            // comments
            "truncate /*some comments,*/table/*some comments*/ `bbb`",
            //  escapes + spaces + comments
            "truncate /*some comments,*/table/*some comments*/     `bbb`  ",
        ];

        let expect_sqls = vec![
            // schema.table
            "TRUNCATE TABLE `bbb`",
            // escapes
            "TRUNCATE TABLE `bbb`",
            // spaces
            "TRUNCATE TABLE `bbb`",
            // spaces + escapes
            "TRUNCATE TABLE `bbb`",
            // comments
            "TRUNCATE TABLE `bbb`",
            //  escapes + spaces + comments
            "TRUNCATE TABLE `bbb`",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.schema, None);
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_rename_table_with_schema() {
        let sqls = vec![
            // schema.table
            "rename table aaa.bbb to aaa.ccc",
            // escapes
            "rename table `aaa`.`bbb` to aaa.ccc",
            // spaces
            "  rename   table  aaa . bbb   to aaa.ccc",
            // spaces + escapes
            "  rename   table  `aaa` . `bbb`   to aaa.ccc",
            // comments
            "rename /*some comments,*/table/*some comments*/ `aaa`.`bbb` to aaa.ccc",
            //  escapes + spaces + comments
            "rename /*some comments,*/table/*some comments*/   `aaa` .  `bbb`   to aaa.ccc",
        ];

        let expect_sqls = vec![
            // schema.table
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
            // escapes
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
            // spaces
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
            // spaces + escapes
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
            // comments
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
            //  escapes + spaces + comments
            "RENAME TABLE `aaa`.`bbb` to aaa.ccc",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::RenameTable);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_rename_table_without_schema() {
        let sqls = vec![
            // schema.table
            "truncate table bbb to ccc",
            // escapes
            "truncate table `bbb` to ccc",
            // spaces
            "  truncate   table   bbb   to ccc",
            // spaces + escapes
            "  truncate   table   `bbb`   to ccc",
            // comments
            "truncate /*some comments,*/table/*some comments*/ `bbb` to ccc",
            //  escapes + spaces + comments
            "truncate /*some comments,*/table/*some comments*/     `bbb`   to ccc",
        ];

        let expect_sqls = vec![
            // schema.table
            "TRUNCATE TABLE `bbb` to ccc",
            // escapes
            "TRUNCATE TABLE `bbb` to ccc",
            // spaces
            "TRUNCATE TABLE `bbb` to ccc",
            // spaces + escapes
            "TRUNCATE TABLE `bbb` to ccc",
            // comments
            "TRUNCATE TABLE `bbb` to ccc",
            //  escapes + spaces + comments
            "TRUNCATE TABLE `bbb` to ccc",
        ];

        for i in 0..sqls.len() {
            let r = DdlParser::parse(sqls[i]).unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.schema, None);
            assert_eq!(r.tb, Some("bbb".to_string()));
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_schema() {
        let sqls = vec![
            "create schema aaa",
            // escapes
            "create schema \"aaa\"",
            // spaces
            "  create   schema   aaa",
            // spaces + escapes
            "  create   schema   \"aaa\"  ",
            // if exists
            "create schema if  not  exists \"aaa\"",
            // comments
            "create /*some comments,*/schema/*some comments*/ \"aaa\"",
            //  escapes + spaces + if exists + comments
            "create /*some comments,*/schema/*some comments*/ if  not  exists    \"aaa\"  ",
        ];

        let expect_sqls = vec![
            r#"CREATE SCHEMA "aaa""#,
            // escapes
            r#"CREATE SCHEMA "aaa""#,
            // spaces
            r#"CREATE SCHEMA "aaa""#,
            // spaces + escapes
            r#"CREATE SCHEMA "aaa""#,
            // if exists
            r#"CREATE SCHEMA IF NOT EXISTS "aaa""#,
            // comments
            r#"CREATE SCHEMA "aaa""#,
            //  escapes + spaces + if exists + comments
            r#"CREATE SCHEMA IF NOT EXISTS "aaa""#,
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = DbType::Pg;
            assert_eq!(r.ddl_type, DdlType::CreateSchema);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_schema_with_special_characters() {
        let sqls = vec![
            "CREATE SCHEMA IF NOT EXISTS \"test_db_*.*\";",
            "CREATE SCHEMA IF NOT EXISTS \"中文.others*&^%$#@!+_)(&^%#\";",
        ];
        let dbs = vec!["test_db_*.*", "中文.others*&^%$#@!+_)(&^%#"];

        let expect_sqls = vec![
            "CREATE SCHEMA IF NOT EXISTS \"test_db_*.*\" ;",
            "CREATE SCHEMA IF NOT EXISTS \"中文.others*&^%$#@!+_)(&^%#\" ;",
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = DbType::Pg;
            assert_eq!(r.ddl_type, DdlType::CreateSchema);
            assert_eq!(r.schema, Some(dbs[i].to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_schema() {
        let sqls = vec![
            "drop schema aaa",
            // escapes
            "drop schema \"aaa\"",
            // spaces
            "  drop   schema   aaa",
            // spaces + escapes
            "  drop   schema   \"aaa\"  ",
            // if exists
            "drop schema if  exists \"aaa\"",
            // comments
            "drop /*some comments,*/schema/*some comments*/ \"aaa\"",
            //  escapes + spaces + if exists + comments
            "drop /*some comments,*/schema/*some comments*/ if  exists    \"aaa\"  ",
        ];

        let expect_sqls = vec![
            r#"DROP SCHEMA "aaa""#,
            // escapes
            r#"DROP SCHEMA "aaa""#,
            // spaces
            r#"DROP SCHEMA "aaa""#,
            // spaces + escapes
            r#"DROP SCHEMA "aaa""#,
            // if exists
            r#"DROP SCHEMA IF EXISTS "aaa""#,
            // comments
            r#"DROP SCHEMA "aaa""#,
            //  escapes + spaces + if exists + comments
            r#"DROP SCHEMA IF EXISTS "aaa""#,
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = DbType::Pg;
            assert_eq!(r.ddl_type, DdlType::DropSchema);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_schema() {
        let sqls = vec![
            "alter schema aaa rename to bbb",
            // escapes
            "alter schema \"aaa\" rename to bbb",
            // spaces
            "  alter   schema   aaa rename to bbb",
            // spaces + escapes
            "  alter   schema   \"aaa\"   rename to bbb",
            // comments
            "alter /*some comments,*/schema/*some comments*/ \"aaa\" rename to bbb",
            //  escapes + spaces + comments
            "alter /*some comments,*/schema/*some comments*/    \"aaa\"   rename to bbb",
        ];

        let expect_sqls = vec![
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
            // escapes
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
            // spaces
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
            // spaces + escapes
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
            // comments
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
            //  escapes + spaces + comments
            r#"ALTER SCHEMA "aaa" rename to bbb"#,
        ];

        for i in 0..sqls.len() {
            let mut r = DdlParser::parse(sqls[i]).unwrap();
            r.db_type = DbType::Pg;
            assert_eq!(r.ddl_type, DdlType::AlterSchema);
            assert_eq!(r.schema, Some("aaa".to_string()));
            assert_eq!(r.tb, None);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }
}

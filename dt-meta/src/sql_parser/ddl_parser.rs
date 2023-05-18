use dt_common::error::Error;
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

use std::{borrow::Cow, str};

use crate::ddl_type::DdlType;

use super::keywords::keyword_a_to_c;
use super::keywords::keyword_c_to_e;
use super::keywords::keyword_e_to_i;
use super::keywords::keyword_i_to_o;
use super::keywords::keyword_o_to_s;
use super::keywords::keyword_s_to_z;

pub struct DdlParser {}

impl DdlParser {
    pub fn parse(sql: &str) -> Result<(DdlType, Option<String>, Option<String>), Error> {
        let sql = Self::remove_comments(sql);
        let input = sql.trim().as_bytes();
        match sql_query(input.as_ref()) {
            Ok((_, o)) => Ok(o),
            Err(_) => Err(Error::Unexpected {
                error: format!("failed to parse sql: {}", sql),
            }),
        }
    }

    fn remove_comments(sql: &str) -> Cow<str> {
        // "create /*some comments,*/table/*some comments*/ `aaa`.`bbb`"
        let regex = Regex::new(r"(/\*([^*]|\*+[^*/*])*\*+/)|(--[^\n]*\n)").unwrap();
        regex.replace_all(&sql, "")
    }
}

/// parse ddl sql and return: (ddl_type, schema, table)
pub fn sql_query(i: &[u8]) -> IResult<&[u8], (DdlType, Option<String>, Option<String>)> {
    alt((
        map(create_database, |r| {
            (DdlType::CreateDatabase, Some(r), None)
        }),
        map(drop_database, |r| (DdlType::DropDatabase, Some(r), None)),
        map(alter_database, |r| (DdlType::AlterDatabase, Some(r), None)),
        map(create_table, |r| (DdlType::CreateTable, r.0, Some(r.1))),
        map(drop_table, |r| (DdlType::DropTable, r.0, Some(r.1))),
        map(alter_table, |r| (DdlType::AlterTable, r.0, Some(r.1))),
        map(truncate_table, |r| (DdlType::TuncateTable, r.0, Some(r.1))),
        map(rename_table, |r| (DdlType::RenameTable, r.0, Some(r.1))),
    ))(i)
}

pub fn create_database(i: &[u8]) -> IResult<&[u8], String> {
    let (remaining_input, (_, _, _, _, _, database, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        opt(if_not_exists),
        sql_identifier,
        multispace0,
    ))(i)?;
    let database = String::from(str::from_utf8(database).unwrap());
    Ok((remaining_input, database))
}

pub fn drop_database(i: &[u8]) -> IResult<&[u8], String> {
    let (remaining_input, (_, _, _, _, _, database, _)) = tuple((
        tag_no_case("drop"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        opt(if_exists),
        sql_identifier,
        multispace0,
    ))(i)?;
    let database = String::from(str::from_utf8(database).unwrap());
    Ok((remaining_input, database))
}

pub fn alter_database(i: &[u8]) -> IResult<&[u8], String> {
    let (remaining_input, (_, _, _, _, database, _)) = tuple((
        tag_no_case("alter"),
        multispace1,
        tag_no_case("database"),
        multispace1,
        sql_identifier,
        multispace1,
    ))(i)?;
    let database = String::from(str::from_utf8(database).unwrap());
    Ok((remaining_input, database))
}

pub fn create_table(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    let (remaining_input, (_, _, _, _, _, table, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(if_not_exists),
        schema_table_reference,
        multispace0,
    ))(i)?;
    Ok((remaining_input, table))
}

pub fn drop_table(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    let (remaining_input, (_, _, _, _, _, table, _)) = tuple((
        tag_no_case("drop"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(if_exists),
        schema_table_reference,
        multispace0,
    ))(i)?;
    Ok((remaining_input, table))
}

pub fn alter_table(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("alter"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        schema_table_reference,
        multispace1,
    ))(i)?;
    Ok((remaining_input, table))
}

pub fn truncate_table(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("truncate"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        schema_table_reference,
        multispace0,
    ))(i)?;
    Ok((remaining_input, table))
}

pub fn rename_table(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    let (remaining_input, (_, _, _, _, table, _)) = tuple((
        tag_no_case("rename"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        schema_table_reference,
        multispace0,
    ))(i)?;
    Ok((remaining_input, table))
}

pub fn if_not_exists(i: &[u8]) -> IResult<&[u8], ()> {
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

pub fn if_exists(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = tuple((
        tag_no_case("if"),
        multispace1,
        tag_no_case("exists"),
        multispace1,
    ))(i)?;
    Ok((remaining_input, ()))
}

// Parse a reference to a named schema.table, with an optional alias
pub fn schema_table_reference(i: &[u8]) -> IResult<&[u8], (Option<String>, String)> {
    map(
        tuple((
            opt(pair(sql_identifier, pair(multispace0, tag(".")))),
            multispace0,
            sql_identifier,
        )),
        |tup| {
            let name = String::from(str::from_utf8(tup.2).unwrap());
            let schema = match tup.0 {
                Some((schema, _)) => Some(String::from(str::from_utf8(schema).unwrap())),
                None => None,
            };
            (schema, name)
        },
    )(i)
}

#[inline]
pub fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '_' as u8 || chr == '@' as u8
}

pub fn sql_identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
        delimited(tag("`"), take_while1(is_sql_identifier), tag("`")),
        delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
    ))(i)
}

// Matches any SQL reserved keyword
pub fn sql_keyword(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        keyword_a_to_c,
        keyword_c_to_e,
        keyword_e_to_i,
        keyword_i_to_o,
        keyword_o_to_s,
        keyword_s_to_z,
    ))(i)
}

#[cfg(test)]
mod test {
    use crate::ddl_type::DdlType;

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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::CreateTable);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::CreateTable);
            assert_eq!(r.1, None);
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::DropTable);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::DropTable);
            assert_eq!(r.1, None);
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::AlterTable);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::AlterTable);
            assert_eq!(r.1, None);
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::CreateDatabase);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, None);
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::DropDatabase);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, None);
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::AlterDatabase);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, None);
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
        ];

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::TuncateTable);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::TuncateTable);
            assert_eq!(r.1, None);
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::RenameTable);
            assert_eq!(r.1, Some("aaa".to_string()));
            assert_eq!(r.2, Some("bbb".to_string()));
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

        for sql in sqls {
            let r = DdlParser::parse(sql).unwrap();
            assert_eq!(r.0, DdlType::TuncateTable);
            assert_eq!(r.1, None);
            assert_eq!(r.2, Some("bbb".to_string()));
        }
    }
}

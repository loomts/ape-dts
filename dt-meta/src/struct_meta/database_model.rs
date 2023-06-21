#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum StructModel {
    DatabaseModel {
        name: String,
        // character_set: CharacterSetType,
    },
    SchemaModel {
        database_name: String,
        schema_name: String,
    },
    TableModel {
        database_name: String,
        schema_name: String,
        table_name: String,
        engine_name: String, // innodb
        table_comment: String,
        columns: Vec<Column>,
    },
    ConstraintModel {
        database_name: String,
        schema_name: String,
        table_name: String,
        constraint_name: String,
        constraint_type: String,
        definition: String,
    },
    IndexModel {
        database_name: String,
        schema_name: String,
        table_name: String,
        index_name: String,
        index_kind: IndexKind,
        index_type: String, // btree, hash
        comment: String,
        tablespace: String,
        definition: String,
        columns: Vec<IndexColumn>,
    },
    CommentModel {
        comment_type: CommentType,
        database_name: String,
        schema_name: String,
        table_name: String,
        column_name: String,
        comment: String,
    },
    SequenceModel {
        sequence_name: String,
        database_name: String,
        schema_name: String,
        data_type: String,
        start_value: String,
        increment: String,
        min_value: String,
        max_value: String,
        is_circle: String,
    },
    SequenceOwnerModel {
        sequence_name: String,
        database_name: String,
        schema_name: String,
        owner_table_name: String,
        owner_table_column_name: String,
    },
    ViewModel {},
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub column_name: String,
    pub order_position: u32,
    pub default_value: Option<String>,
    pub is_nullable: String,
    pub column_type: String, // varchar(100)
    pub column_key: String,  // PRI, MUL
    pub extra: String,       // auto_increment
    pub column_comment: String,
    pub generated: Option<String>,
    pub character_set: String,
    pub collation: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexColumn {
    pub column_name: String,
    pub seq_in_index: u32,
}

#[derive(Clone, Debug)]
pub enum IndexKind {
    PrimaryKey,
    Unique,
    Index,
    Unkown,
}

impl PartialEq for IndexKind {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (IndexKind::PrimaryKey, IndexKind::PrimaryKey)
                | (IndexKind::Unique, IndexKind::Unique)
                | (IndexKind::Index, IndexKind::Index)
        )
    }
}

#[derive(Clone, Debug)]
pub enum CommentType {
    Table,
    Column,
}

impl PartialEq for CommentType {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (CommentType::Table, CommentType::Table) | (CommentType::Column, CommentType::Column)
        )
    }
}

impl PartialEq for StructModel {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                StructModel::DatabaseModel { name: name1 },
                StructModel::DatabaseModel { name: name2 },
            ) => name1 == name2,
            (
                StructModel::SchemaModel {
                    database_name: db1,
                    schema_name: schema1,
                },
                StructModel::SchemaModel {
                    database_name: db2,
                    schema_name: schema2,
                },
            ) => db1 == db2 && schema1 == schema2,
            (
                StructModel::TableModel {
                    database_name: db1,
                    schema_name: schema1,
                    table_name: table1,
                    engine_name: engine1,
                    table_comment: comment1,
                    columns: column1,
                },
                StructModel::TableModel {
                    database_name: db2,
                    schema_name: schema2,
                    table_name: table2,
                    engine_name: engine2,
                    table_comment: comment2,
                    columns: column2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    db1 == db2
                        && table1 == table2
                        && engine1 == engine2
                        && comment1 == comment2
                        && column1 == column2
                } else {
                    // such as postgresql
                    schema1 == schema2
                        && table1 == table2
                        && engine1 == engine2
                        && comment1 == comment2
                        && column1 == column2
                }
            }
            (
                StructModel::ConstraintModel {
                    database_name: db1,
                    schema_name: schema1,
                    table_name: table1,
                    constraint_name: constraint1,
                    constraint_type: type1,
                    definition: d1,
                },
                StructModel::ConstraintModel {
                    database_name: db2,
                    schema_name: schema2,
                    table_name: table2,
                    constraint_name: constraint2,
                    constraint_type: type2,
                    definition: d2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    db1 == db2
                        && table1 == table2
                        && constraint1 == constraint2
                        && type1 == type2
                        && d1 == d2
                } else {
                    // such as postgresql
                    schema1 == schema2
                        && table1 == table2
                        && constraint1 == constraint2
                        && type1 == type2
                        && d1 == d2
                }
            }
            (
                StructModel::IndexModel {
                    database_name: db1,
                    schema_name: schema1,
                    table_name: table1,
                    index_name: index1,
                    index_kind: kind1,
                    index_type: type1,
                    comment: comment1,
                    tablespace: space1,
                    definition: d1,
                    columns: c1,
                },
                StructModel::IndexModel {
                    database_name: db2,
                    schema_name: schema2,
                    table_name: table2,
                    index_name: index2,
                    index_kind: kind2,
                    index_type: type2,
                    comment: comment2,
                    tablespace: space2,
                    definition: d2,
                    columns: c2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    db1 == db2
                        && table1 == table2
                        && index1 == index2
                        && kind1 == kind2
                        && type1 == type2
                        && comment1 == comment2
                        && space1 == space2
                        && d1 == d2
                        && c1 == c2
                } else {
                    // such as postgresql
                    schema1 == schema2
                        && table1 == table2
                        && index1 == index2
                        && kind1 == kind2
                        && type1 == type2
                        && comment1 == comment2
                        && space1 == space2
                        && d1 == d2
                        && c1 == c2
                }
            }
            (
                StructModel::CommentModel {
                    comment_type: ct1,
                    database_name: db1,
                    schema_name: schema1,
                    table_name: table1,
                    column_name: col1,
                    comment: c1,
                },
                StructModel::CommentModel {
                    comment_type: ct2,
                    database_name: db2,
                    schema_name: schema2,
                    table_name: table2,
                    column_name: col2,
                    comment: c2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    ct1 == ct2 && db1 == db2 && table1 == table2 && col1 == col2 && c1 == c2
                } else {
                    // such as postgresql
                    ct1 == ct2 && schema1 == schema2 && table1 == table2 && col1 == col2 && c1 == c2
                }
            }
            (
                StructModel::SequenceModel {
                    sequence_name: seq1,
                    database_name: db1,
                    schema_name: schema1,
                    data_type: type1,
                    start_value: s1,
                    increment: i1,
                    min_value: min1,
                    max_value: max1,
                    is_circle: c1,
                },
                StructModel::SequenceModel {
                    sequence_name: seq2,
                    database_name: db2,
                    schema_name: schema2,
                    data_type: type2,
                    start_value: s2,
                    increment: i2,
                    min_value: min2,
                    max_value: max2,
                    is_circle: c2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    db1 == db2
                        && seq1 == seq2
                        && type1 == type2
                        && s1 == s2
                        && i1 == i2
                        && min1 == min2
                        && max1 == max2
                        && c1 == c2
                } else {
                    // such as postgresql
                    schema1 == schema2
                        && seq1 == seq2
                        && type1 == type2
                        // && s1 == s2 // startvalue is not match most of time
                        && i1 == i2
                        && min1 == min2
                        && max1 == max2
                        && c1 == c2
                }
            }
            (
                StructModel::SequenceOwnerModel {
                    sequence_name: seq1,
                    database_name: db1,
                    schema_name: schema1,
                    owner_table_name: ot1,
                    owner_table_column_name: oc1,
                },
                StructModel::SequenceOwnerModel {
                    sequence_name: seq2,
                    database_name: db2,
                    schema_name: schema2,
                    owner_table_name: ot2,
                    owner_table_column_name: oc2,
                },
            ) => {
                if schema1.is_empty() && schema2.is_empty() {
                    // suck as mysql
                    db1 == db2 && seq1 == seq2 && ot1 == ot2 && oc1 == oc2
                } else {
                    // such as postgresql
                    schema1 == schema2 && seq1 == seq2 && ot1 == ot2 && oc1 == oc2
                }
            }
            _ => false,
        }
    }
}

impl StructModel {
    pub fn to_log_string(&self) -> String {
        match self {
            Self::DatabaseModel { name } => format!("database:[name:{}]", name),
            Self::SchemaModel {
                database_name,
                schema_name,
            } => {
                format!(
                    "schema:[database:{}, schema:{}]",
                    database_name, schema_name
                )
            }
            Self::TableModel {
                database_name,
                schema_name,
                table_name,
                engine_name: _,
                table_comment: _,
                columns: _,
            } => {
                format!(
                    "table:[database:{}, schema:{}, table_name:{}]",
                    database_name, schema_name, table_name
                )
            }
            Self::IndexModel {
                database_name,
                schema_name,
                table_name,
                index_name,
                index_kind: _,
                index_type: _,
                comment: _,
                tablespace: _,
                definition: _,
                columns: _,
            } => {
                format!(
                    "index:[database:{}, schema:{}, table:{}, index:{}]",
                    database_name, schema_name, table_name, index_name
                )
            }
            Self::ConstraintModel {
                database_name,
                schema_name,
                table_name,
                constraint_name,
                constraint_type: _,
                definition: _,
            } => {
                format!(
                    "constraint:[database:{}, schema:{}, table:{}, constaint:{}]",
                    database_name, schema_name, table_name, constraint_name
                )
            }
            Self::CommentModel {
                comment_type: _,
                database_name,
                schema_name,
                table_name,
                column_name,
                comment: _,
            } => {
                format!(
                    "comment:[database:{}, schema:{}, table:{}, column:{}]",
                    database_name, schema_name, table_name, column_name
                )
            }
            Self::SequenceModel {
                sequence_name,
                database_name,
                schema_name,
                data_type: _,
                start_value: _,
                increment: _,
                min_value: _,
                max_value: _,
                is_circle: _,
            } => {
                format!(
                    "sequence:[database:{}, schema:{}, sequence:{}]",
                    database_name, schema_name, sequence_name
                )
            }
            Self::SequenceOwnerModel {
                sequence_name,
                database_name,
                schema_name,
                owner_table_name,
                owner_table_column_name,
            } => {
                format!(
                    "sequence-owner:[database:{}, schema:{}, sequence:{}, table:{}, col:{}]",
                    database_name,
                    schema_name,
                    sequence_name,
                    owner_table_name,
                    owner_table_column_name
                )
            }
            _ => format!("{:?}", self),
        }
    }
}

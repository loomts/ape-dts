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
    ViewModel {},
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct IndexColumn {
    pub column_name: String,
    pub seq_in_index: u32,
}

#[derive(Clone, Debug)]
pub enum IndexKind {
    PrimaryKey,
    Unique,
    Index,
}

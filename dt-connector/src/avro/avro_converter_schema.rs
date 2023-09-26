use apache_avro::Schema;

pub struct AvroConverterSchema {}

impl AvroConverterSchema {
    pub fn get_row_data_schema() -> Schema {
        Schema::parse_str(
            r#"
            {
                "namespace": "ape.dts.avro",
                "type": "record",
                "name": "RowData",
                "fields": [{
                        "name": "schema",
                        "type": "string"
                    },
                    {
                        "name": "tb",
                        "type": "string"
                    },
                    {
                        "name": "row_type",
                        "type": "string"
                    },
                    {
                        "name": "before",
                        "type": {
                            "namespace": "ape.dts.avro",
                            "type": "record",
                            "name": "ColValue",
                            "fields": [{
                                    "name": "string_cols",
                                    "type": "map",
                                    "values": "string"
                                },
                                {
                                    "name": "long_cols",
                                    "type": "map",
                                    "values": "long"
                                },
                                {
                                    "name": "double_cols",
                                    "type": "map",
                                    "values": "double"
                                },
                                {
                                    "name": "bytes_cols",
                                    "type": "map",
                                    "values": "bytes"
                                },
                                {
                                    "name": "boolean_cols",
                                    "type": "map",
                                    "values": "boolean"
                                },
                                {
                                    "name": "null_cols",
                                    "type": "map",
                                    "values": "null"
                                }
                            ]
                        }
                    },
                    {
                        "name": "after",
                        "type": "ColValue"
                    }
                ]
            }"#,
        )
        .unwrap()
    }
}

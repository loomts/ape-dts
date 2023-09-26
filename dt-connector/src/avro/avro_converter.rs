use std::{collections::HashMap, str::FromStr};

use apache_avro::{from_avro_datum, to_avro_datum, types::Value, Schema};
use dt_common::error::Error;
use dt_meta::{
    col_value::ColValue, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType,
};

use super::avro_converter_schema::AvroConverterSchema;

#[derive(Clone)]
pub struct AvroConverter {
    schema: Schema,
    pub meta_manager: Option<RdbMetaManager>,
}

impl AvroConverter {
    pub const BEFORE: &str = "before";
    pub const AFTER: &str = "after";
    pub const ROW_TYPE: &str = "row_type";
    pub const SCHEMA: &str = "schema";
    pub const TB: &str = "tb";

    pub const STRING_COLS: &str = "string_cols";
    pub const LONG_COLS: &str = "long_cols";
    pub const DOUBLE_COLS: &str = "double_cols";
    pub const BYTES_COLS: &str = "bytes_cols";
    pub const BOOLEAN_COLS: &str = "boolean_cols";
    pub const NULL_COLS: &str = "null_cols";

    pub fn new(meta_manager: Option<RdbMetaManager>) -> Self {
        AvroConverter {
            schema: AvroConverterSchema::get_row_data_schema(),
            meta_manager,
        }
    }

    pub async fn row_data_to_avro_key(&mut self, row_data: &RowData) -> Result<String, Error> {
        if let Some(meta_manager) = self.meta_manager.as_mut() {
            let tb_meta = meta_manager
                .get_tb_meta(&row_data.schema, &row_data.tb)
                .await?;
            let convert = |col_values: &HashMap<String, ColValue>| {
                if let Some(col) = &tb_meta.order_col {
                    if let Some(value) = col_values.get(col) {
                        return value.to_option_string();
                    }
                }
                None
            };

            if let Some(key) = match row_data.row_type {
                RowType::Insert => convert(&row_data.after.as_ref().unwrap()),
                RowType::Update | RowType::Delete => convert(&row_data.before.as_ref().unwrap()),
            } {
                return Ok(key);
            }
        }
        Ok(String::new())
    }

    pub fn row_data_to_avro_value(&self, row_data: RowData) -> Result<Vec<u8>, Error> {
        let to_avro_value = |col_values: Option<HashMap<String, ColValue>>| {
            if let Some(value) = col_values {
                Self::col_values_to_avro(&value)
            } else {
                Self::col_values_to_avro(&HashMap::new())
            }
        };

        let before = to_avro_value(row_data.before);
        let after = to_avro_value(row_data.after);

        let value = Value::Record(vec![
            (Self::SCHEMA.into(), Value::String(row_data.schema.into())),
            (Self::TB.into(), Value::String(row_data.tb.into())),
            (
                Self::ROW_TYPE.into(),
                Value::String(row_data.row_type.to_string()),
            ),
            (Self::BEFORE.into(), before),
            (Self::AFTER.into(), after),
        ]);
        Ok(to_avro_datum(&self.schema, value)?)
    }

    pub fn avro_value_to_row_data(&self, payload: Vec<u8>) -> Result<RowData, Error> {
        let mut reader = payload.as_slice();
        let value = from_avro_datum(&self.schema, &mut reader, None)?;
        let mut avro_map = Self::avro_to_map(value);

        let avro_to_string = |value: Option<Value>| {
            if let Some(v) = value {
                if let Value::String(string_v) = v {
                    return string_v;
                }
            }
            String::new()
        };

        let schema = avro_to_string(avro_map.remove(Self::SCHEMA));
        let tb = avro_to_string(avro_map.remove(Self::TB));
        let row_type = avro_to_string(avro_map.remove(Self::ROW_TYPE));
        let before = self.avro_to_col_values(avro_map.remove(Self::BEFORE));
        let after = self.avro_to_col_values(avro_map.remove(Self::AFTER));

        Ok(RowData {
            schema,
            tb,
            row_type: RowType::from_str(&row_type)?,
            before,
            after,
            position: String::new(),
        })
    }

    fn avro_to_col_values(&self, value: Option<Value>) -> Option<HashMap<String, ColValue>> {
        let to_col_values = |avro_map: &mut HashMap<String, Value>, field: &str| {
            let mut col_values = HashMap::new();
            if let Some(avro_cols) = avro_map.remove(field) {
                if let Value::Map(avro_col_map) = avro_cols {
                    for (name, value) in avro_col_map {
                        col_values.insert(name.to_owned(), Self::avro_to_col_value(value));
                    }
                }
            }
            col_values
        };

        if let Some(v) = value {
            let mut avro_map = Self::avro_to_map(v);
            let mut col_values = HashMap::new();
            col_values.extend(to_col_values(&mut avro_map, Self::STRING_COLS));
            col_values.extend(to_col_values(&mut avro_map, Self::LONG_COLS));
            col_values.extend(to_col_values(&mut avro_map, Self::DOUBLE_COLS));
            col_values.extend(to_col_values(&mut avro_map, Self::BYTES_COLS));
            col_values.extend(to_col_values(&mut avro_map, Self::BOOLEAN_COLS));
            col_values.extend(to_col_values(&mut avro_map, Self::NULL_COLS));
            if col_values.is_empty() {
                return None;
            }
            return Some(col_values);
        }
        None
    }

    fn col_values_to_avro(col_values: &HashMap<String, ColValue>) -> Value {
        let mut string_cols = HashMap::new();
        let mut long_cols = HashMap::new();
        let mut double_cols = HashMap::new();
        let mut bytes_cols = HashMap::new();
        let mut boolean_cols = HashMap::new();
        let mut null_cols = HashMap::new();

        for (name, value) in col_values.iter() {
            let avro_value = Self::col_value_to_avro(&value);
            match avro_value {
                Value::String(_) => string_cols.insert(name.into(), avro_value),
                Value::Long(_) => long_cols.insert(name.into(), avro_value),
                Value::Double(_) => double_cols.insert(name.into(), avro_value),
                Value::Bytes(_) => bytes_cols.insert(name.into(), avro_value),
                Value::Boolean(_) => boolean_cols.insert(name.into(), avro_value),
                Value::Null => null_cols.insert(name.into(), avro_value),
                _ => None,
            };
        }

        Value::Record(vec![
            (Self::STRING_COLS.into(), Value::Map(string_cols)),
            (Self::LONG_COLS.into(), Value::Map(long_cols)),
            (Self::DOUBLE_COLS.into(), Value::Map(double_cols)),
            (Self::BYTES_COLS.into(), Value::Map(bytes_cols)),
            (Self::BOOLEAN_COLS.into(), Value::Map(boolean_cols)),
            (Self::NULL_COLS.into(), Value::Map(null_cols)),
        ])
    }

    fn col_value_to_avro(value: &ColValue) -> Value {
        match value {
            ColValue::Tiny(v) => Value::Long(*v as i64),
            ColValue::UnsignedTiny(v) => Value::Long(*v as i64),
            ColValue::Short(v) => Value::Long(*v as i64),
            ColValue::UnsignedShort(v) => Value::Long(*v as i64),
            ColValue::Long(v) => Value::Long(*v as i64),
            ColValue::Year(v) => Value::Long(*v as i64),

            ColValue::UnsignedLong(v) => Value::Long(*v as i64),
            ColValue::LongLong(v) => Value::Long(*v),
            ColValue::Bit(v) => Value::Long(*v as i64),
            ColValue::Set(v) => Value::Long(*v as i64),
            ColValue::Enum(v) => Value::Long(*v as i64),
            // may lose precision
            ColValue::UnsignedLongLong(v) => Value::Long(*v as i64),

            ColValue::Float(v) => Value::Double(*v as f64),
            ColValue::Double(v) => Value::Double(*v),
            ColValue::Blob(v) | ColValue::Json(v) => Value::Bytes(v.clone()),

            ColValue::Decimal(v)
            | ColValue::Time(v)
            | ColValue::Date(v)
            | ColValue::DateTime(v)
            | ColValue::Timestamp(v)
            | ColValue::String(v)
            | ColValue::Set2(v)
            | ColValue::Enum2(v)
            | ColValue::Json2(v) => Value::String(v.clone()),
            ColValue::MongoDoc(v) => Value::String(v.to_string()),

            ColValue::Bool(v) => Value::Boolean(*v),
            ColValue::None => Value::Null,
        }
    }

    fn avro_to_col_value(value: Value) -> ColValue {
        match value {
            Value::Long(v) => ColValue::LongLong(v),
            Value::Double(v) => ColValue::Double(v),
            Value::Bytes(v) => ColValue::Blob(v),
            Value::String(v) => ColValue::String(v),
            Value::Boolean(v) => ColValue::Bool(v),
            Value::Null => ColValue::None,
            // NOT supported
            _ => ColValue::None,
        }
    }

    fn avro_to_map(value: Value) -> HashMap<String, Value> {
        let mut avro_map = HashMap::new();
        if let Value::Record(record) = value {
            for (field, value) in record {
                avro_map.insert(field, value);
            }
        }
        avro_map
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use dt_meta::row_type::RowType;

    const STRING_COL: &str = "string_col";
    const LONG_COL: &str = "long_col";
    const DOUBLE_COL: &str = "double_col";
    const BYTES_COL: &str = "bytes_col";
    const BOOLEAN_COL: &str = "boolean_col";
    const NULL_COL: &str = "null_col";

    #[test]
    fn test_row_data_to_avro() {
        let schema = "db1";
        let tb = "tb1";

        let mut before = HashMap::new();
        before.insert(STRING_COL.into(), ColValue::String("string_before".into()));
        before.insert(LONG_COL.into(), ColValue::LongLong(1));
        before.insert(DOUBLE_COL.into(), ColValue::Double(1.1));
        before.insert(BYTES_COL.into(), ColValue::Blob(vec![1, 2, 3, 4]));
        before.insert(BOOLEAN_COL.into(), ColValue::Bool(false));
        before.insert(NULL_COL.into(), ColValue::None);

        let mut after = HashMap::new();
        after.insert(STRING_COL.into(), ColValue::String("string_after".into()));
        after.insert(LONG_COL.into(), ColValue::LongLong(2));
        after.insert(DOUBLE_COL.into(), ColValue::Double(2.2));
        after.insert(BYTES_COL.into(), ColValue::Blob(vec![5, 6, 7, 8]));
        after.insert(BOOLEAN_COL.into(), ColValue::Bool(true));
        after.insert(NULL_COL.into(), ColValue::None);

        let avro_converter = AvroConverter::new(None);

        let validate = |row_data: RowData| {
            let payload = avro_converter
                .row_data_to_avro_value(row_data.clone())
                .unwrap();
            let decoded_row_data = avro_converter.avro_value_to_row_data(payload).unwrap();
            assert_eq!(row_data, decoded_row_data);
        };

        let mut row_data = RowData {
            schema: schema.into(),
            tb: tb.into(),
            row_type: RowType::Insert,
            before: None,
            after: Some(after),
            position: String::new(),
        };

        // insert
        validate(row_data.clone());
        // update
        row_data.row_type = RowType::Update;
        row_data.before = Some(before);
        validate(row_data.clone());
        // delete
        row_data.row_type = RowType::Delete;
        row_data.after = None;
        validate(row_data.clone());
    }
}

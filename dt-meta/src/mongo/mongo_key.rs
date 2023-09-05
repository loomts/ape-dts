use mongodb::bson::{oid::ObjectId, Bson, DateTime, Document, Timestamp};
use serde::{Deserialize, Serialize};

use super::mongo_constant::MongoConstants;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MongoKey {
    ObjectId(ObjectId),
    String(String),
    Int32(i32),
    Int64(i64),
    JavaScriptCode(String),
    Timestamp(Timestamp),
    DateTime(DateTime),
    Symbol(String),
}

impl MongoKey {
    pub fn from_doc(doc: &Document) -> Option<MongoKey> {
        if let Some(id) = doc.get(MongoConstants::ID) {
            let value = match id {
                Bson::ObjectId(v) => Some(MongoKey::ObjectId(v.clone())),
                Bson::String(v) => Some(MongoKey::String(v.clone())),
                Bson::Int32(v) => Some(MongoKey::Int32(v.clone())),
                Bson::Int64(v) => Some(MongoKey::Int64(v.clone())),
                Bson::JavaScriptCode(v) => Some(MongoKey::JavaScriptCode(v.clone())),
                Bson::Timestamp(v) => Some(MongoKey::Timestamp(v.clone())),
                Bson::DateTime(v) => Some(MongoKey::DateTime(v.clone())),
                Bson::Symbol(v) => Some(MongoKey::Symbol(v.clone())),
                // other types don't derive Hash and Eq
                _ => None,
            };
            return value;
        }
        None
    }
}

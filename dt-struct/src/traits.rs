use async_trait::async_trait;

use crate::{error::Error, meta::common::database_model::StructModel};

#[async_trait]
pub trait StructExtrator {
    fn set_finished(&self) -> Result<(), Error>;
    fn is_finished(&self) -> Result<bool, Error>;

    async fn build_connection(&mut self) -> Result<(), Error>;
    async fn get_sequence(&self) -> Result<(), Error>;
    async fn get_table(&self) -> Result<(), Error>;
    async fn get_constraint(&self) -> Result<(), Error>;
    async fn get_index(&self) -> Result<(), Error>;
    async fn get_comment(&self) -> Result<(), Error>;

    // // charset,case,timezone configuration and so on
    // fn getDbBasicSetting() -> error;

    // fn fetchDatabases() -> error;
    // fn fetchSchemas() -> error;
    // fn fetchTables() -> error;
    // fn fetchIndexs() -> error;
    // fn fetchViews() -> error;
}

#[async_trait]
pub trait StructSinker {
    async fn build_connection(&mut self) -> Result<(), Error>;
    async fn sink_from_queue(&self, model: &mut StructModel) -> Result<(), Error>;
}

use async_trait::async_trait;

use crate::{error::Error, meta::common::database_model::StructModel};

#[async_trait]
pub trait StructExtrator {
    fn set_finished(&self) -> Result<(), Error>;
    fn is_finished(&self) -> Result<bool, Error>;

    async fn build_connection(&mut self) -> Result<(), Error>;
    async fn get_sequence(&mut self) -> Result<Vec<StructModel>, Error>;
    async fn get_table(&self) -> Result<Vec<StructModel>, Error>;
    async fn get_constraint(&self) -> Result<Vec<StructModel>, Error>;
    async fn get_index(&self) -> Result<Vec<StructModel>, Error>;
    async fn get_comment(&self) -> Result<Vec<StructModel>, Error>;

    // // charset,case,timezone configuration and so on
    // fn get_db_basic_setting() -> error;

    // fn get_databases() -> error;
    // fn get_schemas() -> error;
}

#[async_trait]
pub trait StructSinker {
    async fn build_connection(&mut self) -> Result<(), Error>;
    async fn sink_from_queue(&self, model: &mut StructModel) -> Result<(), Error>;
}

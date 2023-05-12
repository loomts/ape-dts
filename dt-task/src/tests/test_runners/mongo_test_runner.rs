use std::collections::{HashMap, HashSet};

use dt_common::{
    config::{
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    constants::MongoConstants,
    error::Error,
    utils::time_util::TimeUtil,
};
use mongodb::{
    bson::{doc, Document},
    options::FindOptions,
    Client,
};
use regex::Regex;

use crate::task_util::TaskUtil;

use super::base_test_runner::BaseTestRunner;

pub struct MongoTestRunner {
    pub base: BaseTestRunner,
    src_mongo_client: Option<Client>,
    dst_mongo_client: Option<Client>,
}

#[allow(dead_code)]
impl MongoTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        let mut src_mongo_client = None;
        let mut dst_mongo_client = None;

        let config = TaskConfig::new(&base.task_config_file);
        match config.extractor {
            ExtractorConfig::MongoSnapshot { url, .. } | ExtractorConfig::MongoCdc { url, .. } => {
                src_mongo_client = Some(TaskUtil::create_mongo_client(&url).await.unwrap());
            }
            _ => {}
        }

        match config.sinker {
            SinkerConfig::Mongo { url, .. } => {
                dst_mongo_client = Some(TaskUtil::create_mongo_client(&url).await.unwrap());
            }
            _ => {}
        }

        Ok(Self {
            base,
            src_mongo_client,
            dst_mongo_client,
        })
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        self.execute_test_ddl_sqls().await?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();
        for sql in self.base.src_dml_sqls.iter() {
            if sql.contains("insertOne") {
                src_insert_sqls.push(sql.clone());
            }
            if sql.contains("updateOne") {
                src_update_sqls.push(sql.clone());
            }
            if sql.contains("deleteOne") {
                src_delete_sqls.push(sql.clone());
            }
        }

        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let db = Self::get_db(&self.base.src_dml_sqls[0]);

        // insert
        self.execute_dmls(src_mongo_client, &db, &src_insert_sqls)
            .await
            .unwrap();
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs().await;

        // update
        self.execute_dmls(src_mongo_client, &db, &src_update_sqls)
            .await
            .unwrap();
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs().await;

        // delete
        self.execute_dmls(src_mongo_client, &db, &src_delete_sqls)
            .await
            .unwrap();
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs().await;

        self.base.wait_task_finish(&task).await
    }

    pub async fn run_snapshot_test(&self) -> Result<(), Error> {
        self.execute_test_ddl_sqls().await?;

        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let db = Self::get_db(&self.base.src_dml_sqls[0]);
        self.execute_dmls(src_mongo_client, &db, &self.base.src_dml_sqls)
            .await?;

        self.base.start_task().await?;

        self.compare_data_for_tbs().await;
        Ok(())
    }

    async fn execute_test_ddl_sqls(&self) -> Result<(), Error> {
        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let dst_mongo_client = self.dst_mongo_client.as_ref().unwrap();
        let src_db = Self::get_db(&self.base.src_ddl_sqls[0]);
        let dst_db = Self::get_db(&self.base.dst_ddl_sqls[0]);

        self.execute_ddls(src_mongo_client, &src_db, &self.base.src_ddl_sqls)
            .await?;
        self.execute_ddls(dst_mongo_client, &dst_db, &self.base.dst_ddl_sqls)
            .await
    }

    async fn execute_ddls(
        &self,
        client: &Client,
        db: &str,
        sqls: &Vec<String>,
    ) -> Result<(), Error> {
        if sqls.is_empty() {
            return Ok(());
        }

        for sql in sqls.iter() {
            if sql.contains("drop") {
                self.execute_drop(client, &db, sql).await.unwrap();
            }
            if sql.contains("createCollection") {
                self.execute_create(client, &db, sql).await.unwrap();
            }
        }
        Ok(())
    }

    async fn execute_dmls(
        &self,
        client: &Client,
        db: &str,
        sqls: &Vec<String>,
    ) -> Result<(), Error> {
        if sqls.is_empty() {
            return Ok(());
        }

        for sql in sqls.iter() {
            if sql.contains("insertOne") {
                self.execute_insert(client, &db, sql).await?;
            }
            if sql.contains("updateOne") {
                self.execute_update(client, &db, sql).await?;
            }
            if sql.contains("deleteOne") {
                self.execute_delete(client, &db, sql).await?;
            }
        }
        Ok(())
    }

    fn get_db(sql: &str) -> String {
        let re = Regex::new(r"use[ ]+(\w+)").unwrap();
        let cap = re.captures(sql).unwrap();
        cap.get(1).unwrap().as_str().to_string()
    }

    fn get_tbs(sqls: &Vec<String>) -> HashSet<String> {
        let mut tbs = HashSet::new();
        let re = Regex::new(r"db.(\w+).insertOne").unwrap();
        for sql in sqls.iter() {
            if let Some(cap) = re.captures(sql) {
                let tb = cap.get(1).unwrap().as_str().to_string();
                tbs.insert(tb);
            }
        }
        tbs
    }

    async fn execute_drop(&self, client: &Client, db: &str, sql: &str) -> Result<(), Error> {
        let re = Regex::new(r"db.(\w+).drop()").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();

        client
            .database(db)
            .collection::<Document>(tb)
            .drop(None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_create(&self, client: &Client, db: &str, sql: &str) -> Result<(), Error> {
        let re = Regex::new(r#"db.createCollection\("(\w+)"\)"#).unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();

        client
            .database(db)
            .create_collection(tb, None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_insert(&self, client: &Client, db: &str, sql: &str) -> Result<(), Error> {
        let re = Regex::new(r"db.(\w+).insertOne\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc = cap.get(2).unwrap().as_str();

        let doc = Document::from(serde_json::from_str(doc).unwrap());
        client
            .database(db)
            .collection::<Document>(tb)
            .insert_one(doc, None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_delete(&self, client: &Client, db: &str, sql: &str) -> Result<(), Error> {
        let re = Regex::new(r"db.(\w+).deleteOne\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc = cap.get(2).unwrap().as_str();

        let doc = Document::from(serde_json::from_str(doc).unwrap());
        client
            .database(db)
            .collection::<Document>(tb)
            .delete_one(doc, None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_update(&self, client: &Client, db: &str, sql: &str) -> Result<(), Error> {
        let re = Regex::new(r"db.(\w+).updateOne\(([\w\W]+),([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let query_doc = cap.get(2).unwrap().as_str();
        let update_doc = cap.get(3).unwrap().as_str();

        let query_doc = Document::from(serde_json::from_str(query_doc).unwrap());
        let update_doc = Document::from(serde_json::from_str(update_doc).unwrap());
        client
            .database(db)
            .collection::<Document>(tb)
            .update_one(query_doc, update_doc, None)
            .await
            .unwrap();
        Ok(())
    }

    async fn compare_data_for_tbs(&self) {
        let db = Self::get_db(&self.base.src_ddl_sqls[0]);
        let tbs = Self::get_tbs(&self.base.src_dml_sqls);
        for tb in tbs.iter() {
            self.compare_tb_data(&db, tb).await;
        }
    }

    async fn compare_tb_data(&self, db: &str, tb: &str) {
        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let dst_mongo_client = self.dst_mongo_client.as_ref().unwrap();

        let src_data = self.fetch_data(src_mongo_client, db, tb).await;
        let dst_data = self.fetch_data(dst_mongo_client, db, tb).await;

        assert_eq!(src_data.len(), dst_data.len());
        for id in src_data.keys() {
            assert_eq!(src_data.get(id), dst_data.get(id));
        }
    }

    async fn fetch_data(&self, client: &Client, db: &str, tb: &str) -> HashMap<String, Document> {
        let collection = client.database(db).collection::<Document>(tb);
        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();
        let mut cursor = collection.find(None, find_options).await.unwrap();

        let mut results = HashMap::new();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let id = doc.get_object_id(MongoConstants::ID).unwrap().to_string();
            results.insert(id, doc);
        }
        results
    }
}

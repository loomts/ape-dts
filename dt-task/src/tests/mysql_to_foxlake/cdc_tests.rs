#[cfg(test)]
mod test {
    use std::str::FromStr;

    use dt_common::{
        config::{sinker_config::SinkerConfig, task_config::TaskConfig},
        error::Error,
    };
    use rusoto_core::Region;
    use rusoto_s3::{ListObjectsV2Request, ObjectIdentifier, S3Client, S3};
    use serial_test::serial;
    use tokio::runtime::Runtime;

    use crate::tests::test_runners::rdb_test_runner::RdbTestRunner;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let test_dir = "mysql_to_foxlake/cdc_basic_test";
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbTestRunner::new(test_dir)).unwrap();
        let (s3_client, bucket, root_dir) = init_s3_client(&runner.base.task_config_file);
        let s3_client = s3_client.unwrap();

        rt.block_on(delete_dir(&s3_client, &bucket, &root_dir));
        rt.block_on(runner.run_foxlake_test(3000, 10000)).unwrap();
        rt.block_on(check_result(&s3_client, &bucket, &root_dir));
    }

    async fn check_result(client: &S3Client, bucket: &str, dir: &str) {
        let keys = list_keys_in_dir(client, bucket, dir).await.unwrap();
        assert_eq!(keys.len(), 9);

        assert!(keys.iter().any(|i| i.contains("/test_db_1/sequence")));
        assert!(keys.iter().any(|i| i.contains("/test_db_1/1/binlog-ddl")));
        assert!(keys.iter().any(|i| i.contains("/test_db_1/1/tb_1/binlog-")));
        assert!(keys.iter().any(|i| i.contains("/test_db_1/1/tb_2/binlog-")));

        assert!(keys.iter().any(|i| i.contains("/test_db_2/sequence")));
        assert!(keys.iter().any(|i| i.contains("/test_db_2/1/binlog-ddl")));
        assert!(keys.iter().any(|i| i.contains("/test_db_2/1/tb_1/binlog-")));
        assert!(keys.iter().any(|i| i.contains("/test_db_2/2/binlog-ddl")));
        assert!(keys.iter().any(|i| i.contains("/test_db_2/2/tb_2/binlog-")));
    }

    fn init_s3_client(task_config_file: &str) -> (Option<S3Client>, String, String) {
        let config = TaskConfig::new(task_config_file);
        if let SinkerConfig::Foxlake {
            bucket,
            access_key,
            secret_key,
            region,
            root_dir,
            ..
        } = config.sinker
        {
            let region = Region::from_str(&region).unwrap();
            let credentials = rusoto_credential::StaticProvider::new_minimal(
                access_key.to_owned(),
                secret_key.to_owned(),
            );
            let s3_client =
                S3Client::new_with(rusoto_core::HttpClient::new().unwrap(), credentials, region);
            return (Some(s3_client), bucket, root_dir);
        }
        (None, String::new(), String::new())
    }

    async fn list_keys_in_dir(
        client: &S3Client,
        bucket: &str,
        dir: &str,
    ) -> Result<Vec<String>, Error> {
        let request = ListObjectsV2Request {
            bucket: bucket.to_string(),
            prefix: Some(dir.to_string()),
            ..Default::default()
        };

        let mut objects = client
            .list_objects_v2(request)
            .await
            .unwrap()
            .contents
            .unwrap_or_default();

        let mut keys = Vec::new();
        while let Some(obj) = objects.pop() {
            keys.push(obj.key.unwrap());
        }
        Ok(keys)
    }

    async fn delete_dir(client: &S3Client, bucket: &str, dir: &str) {
        let sub_keys = list_keys_in_dir(client, bucket, dir).await.unwrap();
        let mut objects: Vec<ObjectIdentifier> = Vec::new();
        for key in sub_keys {
            objects.push(ObjectIdentifier {
                key,
                ..Default::default()
            });
        }

        // Delete all objects with the given keys
        if !objects.is_empty() {
            let request = rusoto_s3::DeleteObjectsRequest {
                bucket: bucket.to_string(),
                delete: rusoto_s3::Delete {
                    objects,
                    ..Default::default()
                },
                ..Default::default()
            };
            client.delete_objects(request).await.unwrap();
        }
    }
}

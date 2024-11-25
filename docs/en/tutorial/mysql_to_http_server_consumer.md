# Start as HTTP server and extract MySQL data

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/rdb_to_http_server.md) and [common configs](/docs/en/config.md) for more details.

- Refer to [Start ape_dts as HTTP server to provide data to consumers](/docs/en/consumer/http_consumer.md) for task description.

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# CDC task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@host.docker.internal:3307?ssl-mode=disabled

[sinker]
sink_type=dummy

[parallelizer]
parallel_type=serial
parallel_size=1

[filter]
do_dbs=test_db,test_db_2
do_events=insert,update,delete
do_ddls=*

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
pipeline_type=http_server
http_host=0.0.0.0
http_port=10231
with_field_defs=true
EOL
```

```
docker run --rm \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-p 10231:10231 \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Make changes in MySQL
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

CREATE DATABASE test_db_2;
CREATE TABLE test_db_2.tb_2(id int, value int, primary key(id));
INSERT INTO test_db_2.tb_2 VALUES(1,1);
UPDATE test_db_2.tb_2 SET value=100000 WHERE id=1;
DELETE FROM test_db_2.tb_2;
```

# Start consumer

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)
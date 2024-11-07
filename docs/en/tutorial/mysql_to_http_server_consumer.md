# Start as HTTP server and extract MySQL data

Refer to [Start ape_dts as an HTTP server to provide data to consumers](/docs/en/consumer/http_consumer.md) for details.

# Prerequisites
- [prerequisites](./prerequisites.md)
- python3

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Snapshot task
## Prepare data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@host.docker.internal:3307?ssl-mode=disabled

[sinker]
sink_type=dummy

[parallelizer]
parallel_type=serial
parallel_size=1

[filter]
do_dbs=test_db
do_events=insert

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

## Test HTTP api

curl "http://127.0.0.1:10231/info"
```
{"acked_batch_id":0,"sent_batch_id":0}
```

curl "http://127.0.0.1:10231/fetch_new?batch_size=2"
```
{"data":[[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,2,10,118,97,108,117,101,4,2,0,0],[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,4,10,118,97,108,117,101,4,4,0,0]],"batch_id":1}
```

curl "http://127.0.0.1:10231/fetch_old?old_batch_id=1"
```
{"data":[[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,2,10,118,97,108,117,101,4,2,0,0],[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,4,10,118,97,108,117,101,4,4,0,0]],"batch_id":1}
```

curl -X POST "http://127.0.0.1:10231/ack" -H "Content-Type: application/json" -d '{"ack_batch_id": 1}'
```
{"acked_batch_id":1}
```

curl "http://127.0.0.1:10231/info"
```
{"acked_batch_id":1,"sent_batch_id":1}
```

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
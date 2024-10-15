# Migrate Data from Postgres to Postgres

# Prerequisites
- docker
- python3

# Prepare Postgres Instance
refer to [pg to pg](./pg_to_pg.md)

# Prepare Kafka Instance
refer to [mysql to kafka](./pg_to_kafka_constomer.md)

# Send Snapshot Data to Kafka
## prepare data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=kafka
sink_type=write
url=127.0.0.1:9093
with_field_defs=true

[filter]
do_dbs=test_db
do_events=insert

[router]
topic_map=*.*:test

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

# Send Cdc Data to Kafka
## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

[filter]
do_dbs=test_db,test_db_2
do_events=insert,update,delete
do_ddls=*

[router]
topic_map=*.*:test

[sinker]
db_type=kafka
sink_type=write
url=127.0.0.1:9093
with_field_defs=true

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

## make changes in postgres
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db_2;
CREATE TABLE test_db_2.tb_2(id int, value int, primary key(id));
INSERT INTO test_db_2.tb_2 VALUES(1,1);
UPDATE test_db_2.tb_2 SET value=100000 WHERE id=1;
DELETE FROM test_db_2.tb_2;
```

# Run Kafka Customer Demo
- [refer to demo](https://github.com/apecloud/cubetran_udf_python)
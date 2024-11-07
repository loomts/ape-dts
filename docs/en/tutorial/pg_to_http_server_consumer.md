# Start as HTTP server and extract Postgres data

Refer to [Start ape_dts as an HTTP server to provide data to consumers](/docs/en/consumer/http_consumer.md) for details.

# Prerequisites
- [prerequisites](./prerequisites.md)
- python3

# Prepare Postgres instances
Refer to [pg to pg](./pg_to_pg.md)

# Snapshot task
## Prepare data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@host.docker.internal:5433/postgres?options[statement_timeout]=10s

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

# CDC task
## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@host.docker.internal:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

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

## Make changes in Postgres
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db_2;
CREATE TABLE test_db_2.tb_2(id int, value int, primary key(id));
INSERT INTO test_db_2.tb_2 VALUES(1,1);
UPDATE test_db_2.tb_2 SET value=100000 WHERE id=1;
DELETE FROM test_db_2.tb_2;
```

# Start consumer

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)
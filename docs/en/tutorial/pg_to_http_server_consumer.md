# Start as HTTP server and extract Postgres data

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/rdb_to_http_server.md) and [common configs](/docs/en/config.md) for more details.

- Refer to [Start ape_dts as HTTP server to provide data to consumers](/docs/en/consumer/http_consumer.md) for task description.

# Prepare Postgres instances
Refer to [pg to pg](./pg_to_pg.md)

# CDC task

## Drop replication slot if exists
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
```

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
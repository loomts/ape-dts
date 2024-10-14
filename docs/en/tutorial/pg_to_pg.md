# Migrate Data from Postgres to Postgres

# Prerequisites
- docker

# Prepare Postgres Instances

## Source
```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d postgis/postgis:latest
```

- set wal_level to logical

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5432 -W

ALTER SYSTEM SET wal_level = logical;

-- restart container
docker restart some-postgres-1
```

## Target

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d postgis/postgis:latest
```

# Migrate Structures

## prepare data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## start task
```
rm -rf /tmp/ape_dts
mkdir -p /tmp/ape_dts

cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
sink_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

## check results
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SET search_path TO test_db;
\d
```

```
         List of relations
 Schema  | Name | Type  |  Owner   
---------+------+-------+----------
 test_db | tb_1 | table | postgres
```

# Migrate Snapshot Data
## prepare data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

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
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db
do_events=insert

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

# check results
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
 id | value 
----+-------
  1 |     1
  2 |     2
  3 |     3
  4 |     4
```

# Check Data
- check the differences between target data and source data

## prepare data
- change target table records
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=1 WHERE id=2;
```

## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=pg
sink_type=check
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/:/logs/" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

## check results
- cat /tmp/ape_dts/check_data_task_log/check/miss.log
```
{"log_type":"Miss","schema":"test_db","tb":"tb_1","id_col_values":{"id":"1"},"diff_col_values":{}}
```
- cat /tmp/ape_dts/check_data_task_log/check/diff.log
```
{"log_type":"Diff","schema":"test_db","tb":"tb_1","id_col_values":{"id":"2"},"diff_col_values":{"value":{"src":"2","dst":"1"}}}
```

# Revise Data
- revise target data based on "check data" task results

## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./check_data_task_log

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

## check results
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
+----+-------+
| id | value |
+----+-------+
|  1 |     1 |
|  2 |     2 |
|  3 |     3 |
|  4 |     4 |
+----+-------+
```

# Review Data
- check if target data revised based on "check data" task results

## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./check_data_task_log

[sinker]
db_type=pg
sink_type=check
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
-v "/tmp/ape_dts/review_data_task_log/:/logs/" \
apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/ape-dts:distross.1 /task_config.ini 
```

## check results
- /tmp/ape_dts/review_data_task_log/check/miss.log and /tmp/ape_dts/review_data_task_log/check/diff.log should be empty

# Cdc Task

## start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=pg
sink_type=write
batch_size=200
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[parallelizer]
parallel_type=rdb_merge
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

## change data in source table
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## check results
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
+----+---------+
| id | value   |
+----+---------+
|  2 | 2000000 |
|  3 |       3 |
|  4 |       4 |
|  5 |       5 |
+----+---------+
```
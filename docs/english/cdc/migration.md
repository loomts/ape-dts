# Introduction

Subscribe to data changes in the source database and sync them to the target.

Prerequisites
    - MySQL: Enables binlog in the source database;
    - PG: Sets `wal_level = logical` in the source database;
    - Mongo: The source instance must be ReplicaSet;
    - For more informaiton, refer to [init test env](../../../dt-tests/README.md).

# Example: MySQL_to_MySQL

```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=637309
binlog_filename=mysql-bin.000006
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# Parallel computing

- MySQL/PG: parallel_type=rdb_merge
- Mongo: parallel_type=mongo
- Redis: parallel_type=redis

# Other configurations

- For [filter] and [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/cdc
    - dt-tests/tests/pg_to_pg/cdc
    - dt-tests/tests/mongo_to_mongo/cdc
    - dt-tests/tests/redis_to_redis/cdc

- Modify performance parameters if needed:
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
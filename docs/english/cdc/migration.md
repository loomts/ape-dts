# Introduction
- Subscribe to data changes in source and sync them to target

- Preconditions
    - mysql: enables binlog in source
    - pg: set wal_level = logical in source
    - mongo: The source instance must be ReplicaSet
    - Also refer to: [init test env](../../../dt-tests/README.md)

# Example: mysql_to_mysql
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
field_map=
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

# Parallel
- mysql/pg: parallel_type=rdb_merge
- mongo: parallel_type=mongo
- redis: parallel_type=redis

# Other configs
- [filter], [router]: refer to [config details](../config.md)
- Also refer to task_config.ini in tests:
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
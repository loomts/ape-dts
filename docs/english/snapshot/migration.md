# Introduction
- If the snapshot task contains multiple databases/tables, tables will be sorted first by database name and then table name, then they will be migrated to target one by one
- If the table has a single primary/unique key, the extractor will use this key as the sorting column and pull data in batches, batch size is configured by [pipeline] buffer_size
- If the table does not have a sorting column, the extractor will pull all data in stream

# Example: mysql_to_mysql
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
field_map=

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# Parallel
- redis_to_redis: parallel_type=redis
- others: parallel_type=snapshot

# Other configs
- [filter]，[router]: refer to [config details](../config.md)
- Also refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/snapshot
    - dt-tests/tests/pg_to_pg/snapshot
    - dt-tests/tests/mongo_to_mongo/snapshot
    - dt-tests/tests/redis_to_redis/snapshot

- Modify performance parameters if needed:
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=10000

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```


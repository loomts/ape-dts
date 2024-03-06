# Introduction
- Revise task is based on check results
- Check results specify which rows to be fixed, we still need to get the current data for each row from source, and then fix target with it

# Example: mysql_to_mysql
```
[extractor]
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log
batch_size=200

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
field_map=

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

## Explain
- Differences with snapshot migration config:

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log
```

# Other configs
- [router]: refer to [config details](../config.md)
- Also refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/revise
    - dt-tests/tests/pg_to_pg/revise
    - dt-tests/tests/mongo_to_mongo/revise

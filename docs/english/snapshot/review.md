# Introduction

After data revision, you can review the data again based on the check results.

The check results serve as a guide for specifying the rows/scope to be reviewed, and you still need to get the current data for each row from the source database, to compare it with the target.

# Example: MySQL_to_MySQL
```
[extractor]
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./dt-tests/tests/mysql_to_mysql/review/basic_test/check_log
batch_size=200

[sinker]
db_type=mysql
sink_type=check
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
col_map=

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

## Note

While this configuration is similar to that of snapshot migration, the only differences are:

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log

[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# Other configurations

- For [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/review
    - dt-tests/tests/pg_to_pg/review
    - dt-tests/tests/mongo_to_mongo/review

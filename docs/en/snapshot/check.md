# Introduction

After data migration, you may want to compare the source data and the target data. If there are too many records, try sampling check. Before you start, please ensure that the tables to be verified have primary/unique keys.

MySQL/PG/Mongo are currently supported for data check.

# Example: MySQL_to_MySQL

## Full check
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=check
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

## Sampling check

Based on full check configuration, add `sample_interval` for sampling check. The following code means that every 3 records will be sampled once.

```
[extractor]
sample_interval=3
```

## Note

While this configuration is similar to that of snapshot migration, the only differences are:

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# Results

The results are written to logs in JSON format, including diff.log and miss.log. The logs are stored in the log/check subdirectory.

## diff.log

The diff log includes the database (schema), table (tb), primary key/unique key (id_col_values), and the source and target values of the differing columns (diff_col_values).

```
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## miss.log

The miss log includes the database (schema), table (tb), and primary key/unique key (id_col_values), with empty diff_col_values.

```
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"},"diff_col_values":{}}
```

# Other configurations

- For [filter] and [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
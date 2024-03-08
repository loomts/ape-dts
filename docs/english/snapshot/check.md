# Introduction
- After data migration, compare the source and target data row by row and column by column
- If there are too many records, try sampling check
- Only supports tables with primary/unique keys
- Support: mysql/pg/mongo

# Example: mysql_to_mysql
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
- Based on full check config, add sample_interval
```
[extractor]
sample_interval=3
```

## Explain
- Differences with snapshot migration config:

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# Results
- Results are written into logs in json, including diff.log and miss.log, in log/check folder

## diff.log
- A diff log contains database(schema), table(tb), primary/unique keys(id_col_values), source and target values ​​of different columns(diff_col_values)

```
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## miss.log
- A miss log contains database(schema), table(tb), primary/unique key(id_col_values), diff_col_values is empty

```
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"},"diff_col_values":{}}
```

# Other configs
- [filter], [router]: refer to [config details](../config.md)
- Also refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
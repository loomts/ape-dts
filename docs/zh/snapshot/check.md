# 简介

数据迁移完成后，需要对源数据和目标数据进行逐行逐列比对。如果数据量过大，可以进行抽样校验。请确保需要校验的表具有主键/唯一键。

支持对 MySQL/PG/Mongo 进行比对。

# 示例: MySQL_to_MySQL

## 全量校验
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

## 抽样校验

在全量校验配置下，添加 `sample_interval` 配置。即，每 3 条记录采样 1 次。
```
[extractor]
sample_interval=3
```

## 说明

此配置和全量同步任务的基本一致，两者的不同之处是：

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# 校验结果

校验结果以 json 格式写入日志中，包括 diff.log 和 miss.log。日志存放在 log/check 子目录中。

## 差异日志（diff.log）

差异日志包括库（schema）、表（tb）、主键/唯一键（id_col_values）、差异列的源值和目标值（diff_col_values）。

```
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## 缺失日志（miss.log）

缺失日志包括库（schema）、表（tb）和主键/唯一键（id_col_values），diff_col_values 为空。

```
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"},"diff_col_values":{}}
```

# 反向校验

将 [extractor] 和 [sinker] 配置调换，即可进行反向校验。

# 其他配置

- 支持 [router]，详情请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
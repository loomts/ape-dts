# 简介
- 数据迁移完成后，对源和目标数据进行逐行逐列比对
- 如果数据量过大，也可进行抽样校验
- 需要校验的表，请确保有主键/唯一键
- 支持：mysql/pg/mongo

# 示例: mysql_to_mysql
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
- 在全量校验配置下，添加 sample_interval 配置，代表每 3 条记录采样 1 次
```
[extractor]
sample_interval=3
```

## 说明
- 主要配置和全量同步任务一致，不同处包括：

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# 校验结果
- 校验结果以 json 格式写入日志中，包括 diff.log 和 miss.log，
- 日志在 log/check 子目录中

## diff.log
- 差异日志包括 库（schema），表（tb），主键/唯一键（id_col_values），差异列的源和目标值（diff_col_values）

```
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## miss.log
- 缺失日志包括 库（schema），表（tb），主键/唯一键（id_col_values），diff_col_values 为空

```
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"},"diff_col_values":{}}
```

# 反向校验
- 将 [extractor] 和 [sinker] 配置调换即可

# 其他配置
- 支持 [router]，参考 [配置详解](../config.md)
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
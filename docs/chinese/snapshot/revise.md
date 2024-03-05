# 简介
- 基于校验结果，可发起订正任务
- 校验结果起到指定订正范围的作用，每条数据仍需回查源库以获取当前值，并基于当前值对目标库订正

# 示例: mysql_to_mysql
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

## 说明
- 主要配置和全量同步任务一致，不同处包括：

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log
```

# 其他配置
- 支持 [router]，参考 [配置详解](../config.md)
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/revise
    - dt-tests/tests/pg_to_pg/revise
    - dt-tests/tests/mongo_to_mongo/revise

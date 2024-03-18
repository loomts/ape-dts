# 简介

订阅源库的数据变更，并同步到目标库。

前提条件
    - MySQL：源库开启 binlog；
    - PG：源库设置 `wal_level = logical`；
    - Mongo：源库需为 ReplicaSet；
    - 详情请参考 [测试环境搭建](../../../dt-tests/README_ZH.md)。

# 示例：MySQL_to_MySQL

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

# 并发算法

- MySQL/PG：parallel_type=rdb_merge
- Mongo：parallel_type=mongo
- Redis：parallel_type=redis

# 其他配置参考

- [filter]、[route] 等配置请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/cdc
    - dt-tests/tests/pg_to_pg/cdc
    - dt-tests/tests/mongo_to_mongo/cdc
    - dt-tests/tests/redis_to_redis/cdc

- 按需修改性能参数：
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
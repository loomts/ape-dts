# 简介
- 全量任务如果包含多个库/多张表，则会按照 **先库后表** 排序，各张表 **依次同步**，有且只有一张表处于同步中
- 如果表具有单一主键/唯一键，则 extractor 会以此键作为排序列，并从小到大分片拉取，每批大小为 [pipeline] 的 buffer_size
- 如果表没有排序列，则 extractor 会流式拉取该表所有数据

# 示例: mysql_to_mysql
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
col_map=

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

# 并发算法
- redis_to_redis：parallel_type=redis
- 其它：parallel_type=snapshot

# 其他配置参考
- [filter]，[router] 等配置参考 [配置详解](../config.md)
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/snapshot
    - dt-tests/tests/pg_to_pg/snapshot
    - dt-tests/tests/mongo_to_mongo/snapshot
    - dt-tests/tests/redis_to_redis/snapshot

- 需根据需要，修改性能参数：
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


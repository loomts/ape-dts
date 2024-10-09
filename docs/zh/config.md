# 示例: MySQL_to_MySQL

## 全量

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
do_tbs=test_db_1.*,test_db_2.*,test_db_3.*
ignore_tbs=
do_events=insert

[router]
db_map=test_db_1:dst_test_db_1
tb_map=test_db_2.one_pk_no_uk_1:dst_test_db_2.dst_one_pk_no_uk_1
col_map=test_db_3.one_pk_no_uk_1.f_0:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_0,test_db_3.one_pk_no_uk_1.f_1:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_1

[pipeline]
buffer_size=16000
buffer_memory_mb=200
checkpoint_interval_secs=10
max_rps=1000

[parallelizer]
parallel_type=snapshot
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

## 增量
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=637309
binlog_filename=mysql-bin.000006
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
heartbeat_interval_secs=1
heartbeat_tb=test_db_1.ape_dts_heartbeat

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*,test_db_3.*
ignore_tbs=
do_events=insert,update,delete

[router]
db_map=test_db_1:dst_test_db_1
tb_map=test_db_2.one_pk_no_uk_1:dst_test_db_2.dst_one_pk_no_uk_1
col_map=test_db_3.one_pk_no_uk_1.f_0:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_0,test_db_3.one_pk_no_uk_1.f_1:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=1000

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# [extractor]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| db_type | 源库类型| mysql |
| extract_type | 拉取类型（全量：snapshot，增量：cdc） | snapshot |
| url | 源库连接信息 | mysql://root:123456@127.0.0.1:3307 |

不同任务类型需要不同的参数，详情请参考各个示例。

# [sinker]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| db_type | 目标库类型| mysql |
| sink_type | 拉取类型（写入：write，校验：check） | write |
| url | 目标库连接信息 | mysql://root:123456@127.0.0.1:3308 |
| batch_size | 批量写入数据条数，1 代表串行 | 200 |

不同任务类型需要不同的参数，详情请参考各个示例。

# [filter]

| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| do_dbs | 需同步的库 | db_1,db_2*,\`db*&#\` |
| ignore_dbs | 需过滤的库 | db_1,db_2*,\`db*&#\` |
| do_tbs | 需同步的表 | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` |
| ignore_tbs | 需过滤的表 | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` |
| do_events | 需同步的事件 | insert、update、delete |

## 取值范围

- 所有配置项均支持多条配置，如 do_dbs 可包含多个库，以 , 分隔。
- 如某配置项需匹配所有条目，则设置成 *，如 do_dbs=\*。
- 如某配置项不匹配任何条目，则设置成空，如 ignore_dbs=。
- do_events 取值：insert、update、delete 中的一个或多个。

## 优先级

- ignore_tbs + ignore_tbs > do_tbs + do_dbs。
- 如果某张表既匹配了 ignore 项，又匹配了 do 项，则该表会被过滤。

## 通配符

| 通配符 | 意义 |
| :-------- | :-------- |
| * | 匹配多个字符 |
| ? | 匹配 0 或 1 个字符 |

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs


## 转义符

| 数据库 | 转义前 | 转义后 |
| :-------- | :-------- | :-------- |
| mysql | db*&# | \`db*&#\` |
| mysql | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg | db*&# | "db*&#" |
| pg | db*&#.tb*$# | "db*&#"."tb*$#" |

如果表名/库名包含特殊字符，需要用相应的转义符括起来。

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs。

# [router]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| db_map | 库级映射 | db_1:dst_db_1,db_2:dst_db_2 |
| tb_map | 表级映射 | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2 |
| col_map | 列级映射 | db_1.tb_1.f_1:dst_db_1.dst_tb_1.dst_f_1,db_1.tb_1.f_2:dst_db_1.dst_tb_1.dst_f_2 |

## 取值范围

- 一个映射规则包括源和目标， 以 : 分隔。
- 所有配置项均支持配置多条，如 db_map 可包含多个库映射，以 , 分隔。
- 如果不配置，则默认 **源库/表/列** 与 **目标库/表/列** 一致，这也是大多数情况。

## 优先级

- tb_map > db_map。
- col_map 只专注于 **列** 映射，而不做 **库/表** 映射。也就是说，如果某张表需要 **库 + 表 + 列** 映射，需先配置好 tb_map 和 db_map，且 col_map 中的 **库/表** 映射规则需和 tb_map/db_map 的映射规则保持一致。

## 通配符

不支持。

## 转义符

和 [filter] 的规则一致。

# [pipeline]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| buffer_size | 内存中最多缓存数据的条数，数据同步采用多线程 & 批量写入，故须配置此项 | 16000 |
| buffer_memory_mb | 可选，缓存数据使用内存上限，如果已超上限，则即使数据条数未达 buffer_size，也将阻塞写入。0 代表不设置 | 200 |
| checkpoint_interval_secs | 任务当前状态（统计数据，同步位点信息等）写入日志的频率，单位：秒 | 10 |
| max_rps | 可选，限制每秒最多同步数据的条数，避免对数据库性能影响 | 1000 |

# [parallelizer]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| parallel_type | 并发类型 | snapshot |
| parallel_size | 并发线程数 | 8 |

## parallel_type 类型

| 类型 | 并行策略 | 适用任务 | 优点 | 缺点 | 
| :-------- | :-------- | :-------- | :-------- | :-------- |
| snapshot | 缓存中的数据分成 parallel_size 份，多线程并行，且批量写入目标 | mysql/pg/mongo 全量 | 快 |  |
| serial | 单线程，依次单条写入目标 | 所有 |  | 慢 |
| rdb_merge | 将缓存中的增量数据（insert, update, delete）整合成 insert + delete 数据，多线程并行，且批量写入目标 | mysql/pg 增量任务 | 快 | 最终一致性，破坏源端事务在目标端重放的完整性 |
| mongo | rdb_merge 的 mongo 版 | mongo 增量 |  |  |
| rdb_check | 和 snapshot 类似，但如果源表没有主键/唯一键，则采用单线程串行写入 | mysql/pg/mongo 全量校验 |  |  |
| redis | 单线程，批量/串行（由 sinker 的 batch_size 决定）写入 | redis 全量/增量 |  |  |

不同任务类型需要不同的 parallel_type，详情请参考各个示例。



# [runtime]
| 配置 | 作用 | 示例 |
| :-------- | :-------- | :-------- |
| log_level | 日志级别 | info/warn/error/debug/trace |
| log4rs_file | log4rs 配置地点，通常不需要改 | ./log4rs.yaml |
| log_dir | 日志输出目录 | ./logs |

通常不需要修改。
# 简介
- 使用范围：mysql，pg
- 迁移内容：database(mysql), schema(pg), table, comment，index, sequence(pg), constraints

# 配置
```
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=mysql
batch_size=1
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
conflict_policy=interrupt

[filter]
do_dbs=struct_it_mysql2mysql_1
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=

[router]
db_map=
tb_map=
field_map=

[parallelizer]
parallel_type=serial
parallel_size=1

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs

[pipeline]
checkpoint_interval_secs=10
buffer_size=100
```

## 说明
- 结构迁移采用单线程串行执行，需注意的配置包括如下：
```
[extractor]
extract_type=struct

[sinker]
sink_type=struct
batch_size=1

[parallelizer]
parallel_type=serial
parallel_size=1
```

- 设置失败策略，包括 interrupt 和 ignore 两种，默认是 interrupt
- interrupt：一旦某个结构迁移失败，任务退出
- ignore：某个结构迁移失败，不影响其他结构继续迁移，但会记录错误日志
```
[sinker]
conflict_policy=interrupt
```

# 分阶段结构迁移
- 在包含 结构迁移 + 数据迁移 的完整数据迁移中，有时为了提升数据迁移的速度，会将步骤拆分成 3 个步骤：
    - 1，迁移 库表结构 + 主键/唯一键，这些是后续数据迁移所必须
    - 2，数据迁移
    - 3，迁移 index/constraints

- 为此，我们提供 2 种 filter 机制（其他配置保持不变）：

## 只迁移 库表结构 + 主键 + 唯一键
```
[filter]
do_structures=database,table
```

## 只迁移 索引 + 约束
```
[filter]
do_structures=constraint,index
```
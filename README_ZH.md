# [English](README.md) | 中文

# 简介

- ape-dts 是一款旨在实现 any-to-any 的数据迁移工具。
- 简单，不依赖第三方组件和额外存储。
- 使用 Rust。

## 支持任务类型

目前支持的成熟任务类型：

<br/>

|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka| mysql -> starrocks |
| :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- |
| 全量迁移 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 增量同步 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 数据校验/订正/复查 | &#10004; | &#10004; | &#10004; | | | | |
| 结构迁移/校验 | &#10004; | &#10004; | | | | |

# 快速上手

## 教程
- [前提条件](./docs/en/tutorial/prerequisites.md)
- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)
- [pg -> pg](./docs/en/tutorial/pg_to_pg.md)
- [mongo -> mongo](./docs/en/tutorial/mongo_to_mongo.md)
- [redis -> redis](./docs/en/tutorial/redis_to_redis.md)
- [mysql -> starrocks](./docs/en/tutorial/mysql_to_starrocks.md)
- [mysql -> kafka -> 消费者](./docs/en/tutorial/mysql_to_kafka_consumer.md)
- [pg -> kafka -> 消费者](./docs/en/tutorial/pg_to_kafka_consumer.md)
- [mysql -> ape_dts(HTTP server) -> 消费者](./docs/en/tutorial/mysql_to_http_server_consumer.md)
- [pg -> ape_dts(HTTP server) -> 消费者](./docs/en/tutorial/pg_to_http_server_consumer.md)
- [全量 + 增量且不丢失数据](./docs/en/tutorial/snapshot_and_cdc_without_data_loss.md)
- [使用 Lua 加工数据](./docs/en/tutorial/etl_by_lua.md)

## 测试用例
- [参考文档](./dt-tests/README_ZH.md)

# 更多文档
- 配置
    - [配置详解](./docs/zh/config.md)
- 库表结构任务
    - [结构迁移](./docs/zh/structure/migration.md)
    - [结构校验](./docs/zh/structure/check.md)
    - [使用 Liquibase 做结构校验](./docs/zh/structure/check_by_liquibase.md)
- 全量任务
    - [迁移](./docs/zh/snapshot/migration.md)
    - [校验](./docs/zh/snapshot/check.md)
    - [订正](./docs/zh/snapshot/revise.md)
    - [复查](./docs/zh/snapshot/review.md)
    - [断点续传](./docs/zh/snapshot/resume.md)
- 增量任务
    - [迁移](./docs/zh/cdc/sync.md)
    - [开启源库心跳](./docs/zh/cdc/heartbeat.md)
    - [双向同步](./docs/zh/cdc/two_way.md)
    - [增量数据转 sql](./docs/zh/cdc/to_sql.md)
    - [断点续传](./docs/zh/cdc/resume.md)
- 自主消费任务
    - [mysql/pg -> kafka -> 消费者](./docs/zh/consumer/kafka_consumer.md)
    - [mysql/pg -> ape_dts(HTTP server) -> 消费者](./docs/zh/consumer/http_consumer.md)
- 数据加工
    - [使用 Lua 加工数据](./docs/zh/etl/lua.md)
- 监控
    - [监控信息](./docs/zh/monitor/monitor.md)
    - [位点信息](./docs/zh/monitor/position.md)
- 任务模版
    - [mysql -> mysql](./docs/templates/mysql_to_mysql.md)
    - [pg -> pg](./docs/templates/pg_to_pg.md)
    - [mongo -> mongo](./docs/templates/mongo_to_mongo.md)
    - [redis -> redis](./docs/templates/redis_to_redis.md)
    - [mysql/pg -> kafka](./docs/templates/rdb_to_kafka.md)
    - [mysql/pg -> ape_dts(HTTP server)](./docs/templates/rdb_to_http_server.md)
    - [mysql -> starrocks](./docs/templates/mysql_to_starrocks.md)

# Benchmark
- MySQL -> MySQL，全量

| 同步方式 | 节点规格 | rps（rows per second) | 源 MySQL 负荷（cpu/内存） | 目标 MySQL 负荷（cpu/内存） |
| :-------- | :-------- | :-------- | :-------- | :-------- | 
| ape_dts | 1c2g | 71428 | 8.2% / 5.2% | 211% / 5.1% |
| ape_dts | 2c4g | 99403 | 14.0% / 5.2% | 359% / 5.1% |
| ape_dts | 4c8g | 126582 | 13.8% / 5.2% | 552% / 5.1% |
| debezium | 4c8g |	4051 | 21.5% / 5.2% | 51.2% / 5.1% |

- MySQL -> MySQL, 增量

| 同步方式 | 节点规格 | rps（rows per second) | 源 MySQL 负荷（cpu/内存） | 目标 MySQL 负荷（cpu/内存） |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 15002 | 18.8% / 5.2% | 467% / 6.5% | 
| ape_dts | 2c4g | 24692 | 18.1% / 5.2% | 687% / 6.5% | 
| ape_dts | 4c8g | 26287 | 18.2% / 5.2% | 685% / 6.5% |
| debezium | 4c8g | 2951 | 20.4% / 5.2% | 98% / 6.5% |

- 镜像对比

| ape_dts:2.0.3	| debezium/connect:2.7 |
| :-------- | :-------- |
| 86.4 MB |	1.38 GB |

- 更多 benchmark [细节](./docs/zh/benchmark.md)

# 开发
## 架构
![架构](docs/img/structure.png)

## 模块
- dt-main：程序启动入口
- dt-connector：各种数据库的 extractor + sinker
- dt-pipeline：串联 extractor 和 sinker 的模块
- dt-parallelizer：各种并发算法
- dt-task：根据配置创建 extractor，sinker，pipeline，parallelizer 以组装任务
- dt-common：通用基础模块，基础数据结构，元数据管理
- dt-tests：集成测试

## 生成镜像
[生成镜像](./docs/en/build_images.md)

# 技术交流
[Slack社区](https://join.slack.com/t/kubeblocks/shared_invite/zt-22cx2f84x-BPZvnLRqBOGdZ_XSjELh4Q)
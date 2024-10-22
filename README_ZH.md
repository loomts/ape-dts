# [English](README.md) | 中文

# 简介

- ape-dts 是一款旨在实现 any-to-any 的数据迁移工具。
- 简单，不依赖第三方组件和额外存储。
- 使用 Rust。

## 支持任务类型

目前支持的成熟任务类型：

<br/>

|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka|
| :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- |
| 全量迁移 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 增量同步 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 数据校验/订正/复查 | &#10004; | &#10004; | &#10004; | | | |
| 结构迁移/校验 | &#10004; | &#10004; | | | |


# 快速上手

## 教程
- [先决条件](./docs/en/tutorial/prerequisites.md)
- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)
- [pg -> pg](./docs/en/tutorial/pg_to_pg.md)
- [mongo -> mongo](./docs/en/tutorial/mongo_to_mongo.md)
- [redis -> redis](./docs/en/tutorial/redis_to_redis.md)
- [mysql -> kafka -> 消费者](./docs/en/tutorial/mysql_to_kafka_consumer.md)
- [pg -> kafka -> 消费者](./docs/en/tutorial/pg_to_kafka_consumer.md)
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
- 数据加工
    - [使用 Lua 加工数据](./docs/zh/etl/lua.md)
- 监控
    - [监控信息](./docs/zh/monitor.md)
    - [位点信息](./docs/zh/position.md)
- 任务模版
    - [mysql -> mysql](./docs/templates/mysql_to_mysql.md)
    - [pg -> pg](./docs/templates/pg_to_pg.md)
    - [mongo -> mongo](./docs/templates/mongo_to_mongo.md)
    - [redis -> redis](./docs/templates/redis_to_redis.md)
    - [mysql -> kafka](./docs/templates/mysql_to_kafka.md)

# Benchmark
- [mysql -> mysql](./docs/zh/benchmark.md)

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

## 写代码

编译及检查
```
cargo build
cargo clippy --workspace
```

确保相关测试通过

## 生成镜像
[生成镜像](./docs/en/build_images.md)

# 技术交流
[Slack社区](https://join.slack.com/t/kubeblocks/shared_invite/zt-22cx2f84x-BPZvnLRqBOGdZ_XSjELh4Q)
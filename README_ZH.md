# [English](README.md) | 中文

# 简介

- ape-dts 是一款旨在实现 any-to-any 的数据迁移工具。
- 简单，不依赖第三方组件和额外存储。
- 使用 Rust。

## 支持任务类型

目前已成熟的任务类型有：

<br/>

|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka|
| :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- |
| 全量迁移 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 增量同步 | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| 数据校验/订正/复查 | &#10004; | &#10004; | &#10004; | | | |
| 结构迁移/校验 | &#10004; | &#10004; | | | |


# 快速上手

## 教程
- [prerequisites](./docs/en/tutorial/prerequisites.md)
- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)
- [pg -> pg](./docs/en/tutorial/pg_to_pg.md)
- [mongo -> mongo](./docs/en/tutorial/mongo_to_mongo.md)
- [redis -> redis](./docs/en/tutorial/redis_to_redis.md)
- [mysql -> kafka -> consumer](./docs/en/tutorial/mysql_to_kafka_consumer.md)
- [pg -> kafka -> consumer](./docs/en/tutorial/pg_to_kafka_consumer.md)
- [snapshot + cdc without data loss](./docs/en/tutorial/snapshot_and_cdc_without_data_loss.md)
- [etl by lua](./docs/en/tutorial/etl_by_lua.md)

## 测试用例
- [参考文档](./dt-tests/README_ZH.md)

# 更多文档
- 配置
    - [配置详解](./docs/zh/config.md)
- 全量任务
    - [迁移](./docs/zh/snapshot/migration.md)
    - [校验](./docs/zh/snapshot/check.md)
    - [订正](./docs/zh/snapshot/revise.md)
    - [复查](./docs/zh/snapshot/review.md)
    - [断点续传](./docs/zh/snapshot/resume.md)
- 增量任务
    - [迁移](./docs/zh/cdc/migration.md)
    - [心跳](./docs/zh/cdc/heartbeat.md)
    - [双向同步](./docs/zh/cdc/two_way.md)
- 数据加工
    - [自定义 lua 脚本](./docs/zh/etl/lua.md)
- 监控
    - [监控](./docs/zh/monitor.md)
    - [位点信息](./docs/zh/position.md)

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
- dt-precheck: 任务预检查，**这部分将被移除**

## 写代码

```
cargo build
cargo clippy --workspace
```

## 创建 docker 镜像

- arm64
```
docker buildx build \
--platform linux/arm64 --tag ape-dts:0.1.0-test-arm64 \
--build-arg MODULE_NAME=dt-main --load . 
```

- amd64
```
docker buildx build \
--platform linux/amd64 --tag ape-dts:0.1.0-test-amd64 \
--build-arg MODULE_NAME=dt-main --load . 
```

# 技术交流
[Slack社区](https://join.slack.com/t/kubeblocks/shared_invite/zt-22cx2f84x-BPZvnLRqBOGdZ_XSjELh4Q)

# English | [中文](README_ZH.md)

# Introduction

- ape-dts is a tool aimed for any-to-any data migration.
- It is lightweight and does not rely on third-party components or additional storage.
- In Rust.


## Supported task types
|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka|
| :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- |
| Snapshot | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| CDC | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| Data check/revise/review | &#10004; | &#10004; | &#10004; | | | |
| Structure migration/check | &#10004; | &#10004; | | | |


# Quick starts

## Tutorial
- [prerequisites](./docs/en/tutorial/prerequisites.md)
- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)
- [pg -> pg](./docs/en/tutorial/pg_to_pg.md)
- [mongo -> mongo](./docs/en/tutorial/mongo_to_mongo.md)
- [redis -> redis](./docs/en/tutorial/redis_to_redis.md)
- [mysql -> kafka -> consumer](./docs/en/tutorial/mysql_to_kafka_consumer.md)
- [pg -> kafka -> consumer](./docs/en/tutorial/pg_to_kafka_consumer.md)
- [snapshot + cdc without data loss](./docs/en/tutorial/snapshot_and_cdc_without_data_loss.md)
- [modify data by lua](./docs/en/tutorial/etl_by_lua.md)

## Run tests

Refer to [docs](./dt-tests/README.md) for details.

# More docs
- Configurations
    - [config details](./docs/en/config.md)
- Structure tasks
    - [migration](./docs/en/structure/migration.md)
    - [check](./docs/en/structure/check.md)
    - [check by Liquibase](./docs/en/structure/check_by_liquibase.md)
- Snapshot tasks
    - [migration](./docs/en/snapshot/migration.md)
    - [check](./docs/en/snapshot/check.md)
    - [revise](./docs/en/snapshot/revise.md)
    - [review](./docs/en/snapshot/review.md)
    - [resume at breakpoint](./docs/en/snapshot/resume.md)
- CDC tasks
    - [data sync](./docs/en/cdc/sync.md)
    - [heartbeat to source database](./docs/en/cdc/heartbeat.md)
    - [two-way data sync](./docs/en/cdc/two_way.md)  
    - [generate sqls from CDC](./docs/en/cdc/to_sql.md)
    - [resume at breakpoint](./docs/en/cdc/resume.md)
- Data processing
    - [modify data by lua](./docs/en/etl/lua.md)
- Monitor
    - [monitor info](./docs/en/monitor.md)
    - [position info](./docs/en/position.md)
- Task Templates
    - [mysql -> mysql](./docs/templates/mysql_to_mysql.md)
    - [pg -> pg](./docs/templates/pg_to_pg.md)
    - [mongo -> mongo](./docs/templates/mongo_to_mongo.md)
    - [redis -> redis](./docs/templates/redis_to_redis.md)
    - [mysql -> kafka](./docs/templates/mysql_to_kafka.md)

# Benchmark
- [mysql -> mysql](./docs/en/benchmark.md)

# Contributions

## Structure

![Structure](docs/img/structure.png)

## Modules
- dt-main: program entry
- dt-connector: extractors + sinkers for databases
- dt-pipeline: pipeline to connect extractors and sinkers
- dt-parallelizer: various parallel algorithms
- dt-task: create extractors + sinkers + pipelines + parallelizers according to configurations
- dt-common: common utils, basic data structures, metadata management
- dt-tests: integration tests

## Coding

Build
```
cargo build
cargo clippy --workspace
```

Make sure all related tests passed.

## Build images
[build images](./docs/en/build_images.md)

# Contact us

[Slack Community](https://join.slack.com/t/kubeblocks/shared_invite/zt-22cx2f84x-BPZvnLRqBOGdZ_XSjELh4Q)
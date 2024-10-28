
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
- [mysql -> ape_dts(HTTP server) -> consumer](./docs/en/tutorial/mysql_to_http_server_consumer.md)
- [pg -> ape_dts(HTTP server) -> consumer](./docs/en/tutorial/pg_to_http_server_consumer.md)
- [snapshot + cdc without data loss](./docs/en/tutorial/snapshot_and_cdc_without_data_loss.md)
- [modify data by lua](./docs/en/tutorial/etl_by_lua.md)

## Run tests

Refer to [test docs](./dt-tests/README.md) for details.

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
- Custom consumers
    - [mysql/pg -> kafka -> consumer](./docs/en/consumer/kafka_consumer.md)
    - [mysql/pg -> ape_dts(HTTP server) -> consumer](./docs/en/consumer/http_consumer.md)
- Data processing
    - [modify data by lua](./docs/en/etl/lua.md)
- Monitor
    - [monitor info](./docs/en/monitor/monitor.md)
    - [position info](./docs/en/monitor/position.md)
- Task Templates
    - [mysql -> mysql](./docs/templates/mysql_to_mysql.md)
    - [pg -> pg](./docs/templates/pg_to_pg.md)
    - [mongo -> mongo](./docs/templates/mongo_to_mongo.md)
    - [redis -> redis](./docs/templates/redis_to_redis.md)
    - [mysql/pg -> kafka](./docs/templates/rdb_to_kafka.md)
    - [mysql/pg -> ape_dts(HTTP server)](./docs/templates/rdb_to_http_server.md)

# Benchmark
- MySQL -> MySQL, Snapshot

| Method | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :-------- | :-------- | :-------- | :-------- | :-------- | 
| ape_dts | 1c2g | 71428 | 8.2% / 5.2% | 211% / 5.1% |
| ape_dts | 2c4g | 99403 | 14.0% / 5.2% | 359% / 5.1% |
| ape_dts | 4c8g | 126582 | 13.8% / 5.2% | 552% / 5.1% |
| debezium | 4c8g |	4051 | 21.5% / 5.2% | 51.2% / 5.1% |

- MySQL -> MySQL, CDC

| Method | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 15002 | 18.8% / 5.2% | 467% / 6.5% | 
| ape_dts | 2c4g | 24692 | 18.1% / 5.2% | 687% / 6.5% | 
| ape_dts | 4c8g | 26287 | 18.2% / 5.2% | 685% / 6.5% |
| debezium | 4c8g | 2951 | 20.4% / 5.2% | 98% / 6.5% |

- more benchmark [details](./docs/en/benchmark.md)

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
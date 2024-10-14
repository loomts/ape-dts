
# English | [中文](README_ZH.md)

# Introduction

- ape-dts is a tool aimed for any-to-any data migration.
- It is lightweight and does not rely on third-party components or additional storage.
- In Rust.


## Tasks supported
|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka|
| :-------- | :-------- | :-------- | :-------- | :-------- | :-------- | :-------- |
| Snapshot | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| CDC | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; | &#10004; |
| Data check/revise/review | &#10004; | &#10004; | &#10004; | | | |
| Structure migration/check | &#10004; | &#10004; | | | |


# Quick starts

## Tutorial

- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)

## Run tests

Refer to [docs](./dt-tests/README.md) for more details.

# More docs
- Configurations
    - [config details](./docs/en/config.md)
- Snapshot tasks
    - [migration](./docs/en/snapshot/migration.md)
    - [check](./docs/en/snapshot/check.md)
    - [revise](./docs/en/snapshot/revise.md)
    - [review](./docs/en/snapshot/review.md)
- CDC tasks
    - [data sync](./docs/en/cdc/migration.md)
    - [heartbeat](./docs/en/cdc/heartbeat.md)
    - [two-way data sync](./docs/en/cdc/two_way.md)
- Data processing
    - [custom lua script](./docs/en/etl/lua.md)

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

```
cargo build
cargo clippy --workspace
```

## Build docker image

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

# Contact us

[Slack Community](https://join.slack.com/t/kubeblocks/shared_invite/zt-22cx2f84x-BPZvnLRqBOGdZ_XSjELh4Q)
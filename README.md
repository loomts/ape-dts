
# English | [中文](README_ZH.md)

# Introduction
- ape-dts is a data migration tool aimed for any-to-any
- simple, does not rely on third-party components or additional storage
- rust


## Current supported tasks

|  | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| snapshot | &#10004; | &#10004; | &#10004; | &#10004; |
| cdc | &#10004; | &#10004; | &#10004; | &#10004; |
| data check/revise/review | &#10004; | &#10004; | &#10004; | |
| structure migration/check | &#10004; | &#10004; |  |  |


# Quick start

## Run demo in docker
- task config is in ini, [config details](./docs/english/config.md), [snapshot data migration](./docs/english/snapshot/migration.md), [cdc data sync](./docs/english/cdc/migration.md)

```
docker run -it \
--entrypoint sh \
-v [absolute-path]/task_config.ini:/task_config.ini \
apecloud/ape-dts:0.1.13.hotfix4 \
-c "/ape-dts /task_config.ini"
```

## Run tests
- [docs](./dt-tests/README.md)

# More docs
- Config
    - [config details](./docs/english/config.md)
- Snapshot tasks
    - [migration](./docs/english/snapshot/migration.md)
    - [check](./docs/english/snapshot/check.md)
    - [revise](./docs/english/snapshot/revise.md)
    - [review](./docs/english/snapshot/review.md)
- Cdc tasks
    - [data sync](./docs/english/cdc/migration.md)
    - [heartbeat](./docs/english/cdc/heartbeat.md)
    - [two-way data sync](./docs/english/cdc/two_way.md)

# Contribution
## Structure
![Structure](docs/pics/structure.png)

## Modules
- dt-main: program entry
- dt-connector: extractors + sinkers for databases
- dt-pipeline: pipeline to connect extractors and sinkers
- dt-parallelizer: various parallel algorithms
- dt-task: create extractors + sinkers + pipelines + parallelizers according to config to assemble tasks
- dt-meta: metadata management and basic data structures
- dt-common: common utils
- dt-tests: integration tests

## Building
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

# Wechat
<div align=center>
<img src="docs/pics/WechatIMG.jpg" width="40%" />
</div>
# [English](README.md) | 中文

# 运行测试用例
```rust
#[tokio::test]
#[serial]
async fn cdc_basic_test() {
    TestBase::run_cdc_test("mysql_to_mysql/cdc/basic_test", 3000, 2000).await;
}
```

- 测试用例包括：
  - task_config.ini
  - src_prepare.sql
  - dst_prepare.sql
  - src_test.sql
  - dst_test.sql

- 一个典型测试用例的步骤：
  - 1，对源库执行 src_prepare.sql。
  - 2，对目标库执行 dst_prepare.sql。
  - 3，启动 数据同步 任务的线程。
  - 4，停顿若干毫秒（可根据测试环境的性能和网络状况，修改测试用例的预设值），等待任务初始化。
  - 5，对源库执行 src_test.sql。
  - 6，对目标库执行 dst_test.sql。
  - 7，停顿若干毫秒，等待数据同步完成。
  - 8，对比源和目标数据。

# 配置
- 所有数据库的 extractor url，sinker url 均配置在 .env 文件，各测试用例的 task_config.ini 中引用。

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

# 测试环境搭建

- 本文均在 mac 上以 docker 搭建测试环境为例。

# Postgres 环境搭建

## 源
[创建 Postgres](/docs/en/tutorial/pg_to_pg.md)

## 如要执行 [charset 相关测试](../dt-tests/tests/pg_to_pg/snapshot/charset_euc_cn_test)
- 在源和目标分别预建数据库 postgres_euc_cn。

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

# MySQL 环境搭建
[创建 MySQL](/docs/en/tutorial/mysql_to_mysql.md)

# Mongo
[创建 Mongo](/docs/en/tutorial/mongo_to_mongo.md)

# Kafka
[创建 Kafka](/docs/en/tutorial/mysql_to_kafka_consumer.md)

# Redis
## 镜像版本
- redis 不同版本的数据格式差距较大，我们支持 2.8 - 7.*，rebloom，rejson。
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8.22
- redislabs/rebloom:2.6.3
- redislabs/rejson:2.6.4
- mac 上无法部署 2.8，rebloom，rejson 镜像，可在 EKS(amazon)/AKS(azure)/ACK(alibaba) 上部署，参考目录：dt-tests/k8s/redis。

## 源

```
docker run --name src-redis-7-0 \
    -p 6380:6379 \
    -d redis:7.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-6-2 \
    -p 6381:6379 \
    -d redis:6.2 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-6-0 \
    -p 6382:6379 \
    -d redis:6.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-5-0 \
    -p 6383:6379 \
    -d redis:5.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-4-0 \
    -p 6384:6379 \
    -d redis:4.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```

## 目标

```
docker run --name dst-redis-7-0 \
    -p 6390:6379 \
    -d redis:7.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-6-2 \
    -p 6391:6379 \
    -d redis:6.2 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-6-0 \
    -p 6392:6379 \
    -d redis:6.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-5-0 \
    -p 6393:6379 \
    -d redis:5.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-4-0 \
    -p 6394:6379 \
    -d redis:4.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```

# Starrocks
## 目标
```
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd \
  --name quickstart starrocks/allin1-ubuntu:3.2.11
```
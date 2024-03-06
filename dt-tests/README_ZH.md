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
  - 1，对源库执行 src_prepare.sql
  - 2，对目标库执行 dst_prepare.sql
  - 3，启动 数据同步 任务的线程
  - 4，停顿若干毫秒（可根据测试环境的性能和网络状况，修改测试用例的预设值），等待任务初始化
  - 5，对源库执行 src_test.sql
  - 6，对目标库执行 dst_test.sql
  - 7，停顿若干毫秒，等待数据同步完成
  - 8，对比源和目标数据

# 配置
- 所有数据库的 extractor url，sinker url 均配置在 .env 文件
- 各测试用例的 task_config.ini 中引用

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

# 测试环境搭建

- 本文均在 mac 上以 docker 搭建测试环境为例

# postgres 环境搭建

## 源
```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d postgis/postgis:latest
```

- 如要执行增量测试，需设置 wal_level

```
- 进入容器
docker exec -it some-postgres-1 bash

- 登录数据库 
psql -h 127.0.0.1 -U postgres -d postgres -p 5432 -W

- 执行 sql
ALTER SYSTEM SET wal_level = logical;

- 退出并重启容器
```

## 目标

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d postgis/postgis:latest
```

## 如要执行 [charset 相关测试](../dt-tests/tests/pg_to_pg/snapshot/charset_euc_cn_test)
- 在源和目标分别预建数据库 postgres_euc_cn

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

# mysql 环境搭建

## 源

```
docker run -d --name some-mysql-1 \
--platform linux/x86_64 \
-it \
-p 3307:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:5.7.40 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --default_time_zone=+08:00
```

## 目标

```
docker run -d --name some-mysql-2 \
--platform linux/x86_64 \
-it \
-p 3308:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:5.7.40 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --default_time_zone=+07:00
```

# mongo

## 源
- 只有 Mongo ReplicaSet 才支持 cdc，故如需执行增量测试，源端需要创建为 ReplicaSet

- 1，创建 docker network

```
docker network create mongo-network
```

- 2，启动 mongo ReplicaSet 的 3 个节点

```
docker run -d --name mongo1 --network mongo-network -p 9042:9042 mongo:6.0 --replSet rs0  --port 9042
docker run -d --name mongo2 --network mongo-network -p 9142:9142 mongo:6.0 --replSet rs0  --port 9142
docker run -d --name mongo3 --network mongo-network -p 9242:9242 mongo:6.0 --replSet rs0  --port 9242
```

- 3，设置 ReplicaSet

```
- 进入其中一个容器
docker exec -it mongo1 bash

- 连接数据库
mongosh --host localhost --port 9042

- 执行 sql
> config = {"_id" : "rs0", "members" : [{"_id" : 0,"host" : "mongo1:9042"},{"_id" : 1,"host" : "mongo2:9142"},{"_id" : 2,"host" : "mongo3:9242"}]}
> rs.initiate(config)
> rs.status()
```

- 4，创建用户

```
> use admin
> db.createUser({user: "ape_dts", pwd: "123456", roles: ["root"]})
```

- 5，修改本地 /etc/hosts

```
127.0.0.1 mongo1 mongo2 mongo3
```

- 6，尝试本地连接

```
mongo "mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0"
```

## 目标

```
docker run -d --name dst-mongo \
	-e MONGO_INITDB_ROOT_USERNAME=ape_dts \
	-e MONGO_INITDB_ROOT_PASSWORD=123456 \
    -p 27018:27017 \
	mongo:6.0
```

# redis
## 镜像版本
- redis 不同版本的数据格式差距较大，我们支持 2.8 - 7.*，rebloom，rejson
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8.22
- redislabs/rebloom:2.6.3
- redislabs/rejson:2.6.4
- mac 上无法部署 2.8，rebloom，rejson 镜像，可在 EKS(amazon)/AKS(azure)/ACK(alibaba) 上部署，参考目录：dt-tests/k8s/redis

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
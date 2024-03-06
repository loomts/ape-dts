# English | [中文](README_ZH.md)

# Run tests
```rust
#[tokio::test]
#[serial]
async fn cdc_basic_test() {
    TestBase::run_cdc_test("mysql_to_mysql/cdc/basic_test", 3000, 2000).await;
}
```

- A test contains: 
  - task_config.ini
  - src_prepare.sql
  - dst_prepare.sql
  - src_test.sql
  - dst_test.sql

- Typical steps for running a test: 
  - 1, execute src_prepare.sql in source database
  - 2, execute dst_prepare.sql in target database
  - 3, start data sync task
  - 4, sleep some milliseconds for task initialization
  - 5, execute src_test.sql in source database
  - 6, execute dst_test.sql in target database
  - 7, sleep some milliseconds for data sync
  - 8, compare data of source and target

# Config
- All database urls are configured in .env file and referenced in task_config.ini of tests

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

# Init test env

- Examples work in docker, mac

# postgres

## source
```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d postgis/postgis:latest
```

- To run cdc test, set wal_level

```
docker exec -it some-postgres-1 bash

psql -h 127.0.0.1 -U postgres -d postgres -p 5432 -W

ALTER SYSTEM SET wal_level = logical;
```

## target

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d postgis/postgis:latest
```

## To run [charset tests](../dt-tests/tests/pg_to_pg/snapshot/charset_euc_cn_test)
- create database "postgres_euc_cn" in both source and target

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

# mysql

## source

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

## target

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

## source
- Mongo source needs to be ReplicaSet which supports cdc

- 1, create docker network

```
docker network create mongo-network
```

- 2, create mongo ReplicaSet containers

```
docker run -d --name mongo1 --network mongo-network -p 9042:9042 mongo:6.0 --replSet rs0  --port 9042
docker run -d --name mongo2 --network mongo-network -p 9142:9142 mongo:6.0 --replSet rs0  --port 9142
docker run -d --name mongo3 --network mongo-network -p 9242:9242 mongo:6.0 --replSet rs0  --port 9242
```

- 3, setup ReplicaSet

```
- enter any container
docker exec -it mongo1 bash

- login mongo
mongosh --host localhost --port 9042

- execute sql
> config = {"_id" : "rs0", "members" : [{"_id" : 0,"host" : "mongo1:9042"},{"_id" : 1,"host" : "mongo2:9142"},{"_id" : 2,"host" : "mongo3:9242"}]}
> rs.initiate(config)
> rs.status()
```

- 4, create user

```
> use admin
> db.createUser({user: "ape_dts", pwd: "123456", roles: ["root"]})
```

- 5, update /etc/hosts on mac

```
127.0.0.1 mongo1 mongo2 mongo3
```

- 6, test connecting

```
mongo "mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0"
```

## target

```
docker run -d --name dst-mongo \
	-e MONGO_INITDB_ROOT_USERNAME=ape_dts \
	-e MONGO_INITDB_ROOT_PASSWORD=123456 \
    -p 27018:27017 \
	mongo:6.0
```

# redis
## images
- Data format varies in different redis versions, we support 2.8 - 7.*, rebloom, rejson
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8.22
- redislabs/rebloom:2.6.3
- redislabs/rejson:2.6.4
- Can not deploy 2.8,rebloom,rejson on mac, you may deploy them in EKS(amazon)/AKS(azure)/ACK(alibaba), refer to: dt-tests/k8s/redis

## source

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

## target

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
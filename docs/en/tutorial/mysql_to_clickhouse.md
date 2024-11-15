# Migrate data from MySQL to Clickhouse

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mysql_to_starrocks.md) and [common configs](/docs/en/config.md) for more details.

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Prepare StarRocks instance
- tested versions: 2.5.4 to 3.2.11

```
docker run -d --name some-clickhouse-server \
--ulimit nofile=262144:262144 \
-p 9100:9000 \
-p 8123:8123 \
-e CLICKHOUSE_USER=admin -e CLICKHOUSE_PASSWORD=123456 \
"$CLICKHOUSE_IMAGE"
```

# Migrate structures
## Prepare source data
```
CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=clickhouse
url=http://admin:123456@127.0.0.1:8123

[filter]
do_dbs=test_db

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```

```

# Migrate snapshot data
## Prepare source data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Prepare target tables
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id INT, value INT) ENGINE=OLAP PRIMARY KEY(id) DISTRIBUTED BY HASH(id);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```
+----+-------+
| id | value |
+----+-------+
|  1 |     1 |
|  2 |     2 |
|  3 |     3 |
|  4 |     4 |
+----+-------+
```

# Cdc task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```
+----+---------+
| id | value   |
+----+---------+
|  2 | 2000000 |
|  3 |       3 |
|  4 |       4 |
|  5 |       5 |
+----+---------+
```

# How it works

We use [Stream Load](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/) to import data from MySQL. You need to configure url (query metadata) and stream_load_url (specify Stream Load port and user info).

When importing data into StarRocks by Stream Load, you need to avoid frequent small-batch imports, as this may cause throttle errors in StarRocks. This can be resolved by configuring batch_sink_interval_secs, refer to [task templates](/docs/templates/mysql_to_starrocks.md). Usually, only CDC tasks need to configure batch_sink_interval_secs.

Stream Load allows importing up to 10GB of data in a single load. You can change the following configurations to adjust the batch data size.

```
[pipeline]
buffer_size=100000
buffer_memory_mb=200

[sinker]
batch_size=5000
```

Refer to [config](/docs/en/config.md) for other common configurations

# Suggested column type mapping

| MySQL | StarRocks |
| :-------- | :-------- |
| tinyint | TINYINT |
| smallint | SMALLINT |
| mediumint | INT |
| int | INT |
| bigint | BIGINT |
| decimal | DECIMAL |
| float | FLOAT |
| double | DOUBLE |
| datetime | DATETIME |
| time | VARCHAR |
| date | DATE |
| year | INT |
| timestamp | VARCHAR |
| char | CHAR |
| varchar | VARCHAR |
| binary | BINARY |
| varbinary | VARBINARY |
| tinytext | CHAR/VARCHAR/STRING/TEXT |
| text | CHAR/VARCHAR/STRING/TEXT |
| mediumtext | CHAR/VARCHAR/STRING/TEXT |
| longtext | CHAR/VARCHAR/STRING/TEXT |
| tinyblob | VARBINARY |
| blob | VARBINARY |
| mediumblob | VARBINARY |
| longblob | VARBINARY |
| enum | VARCHAR |
| set | VARCHAR |
| bit | VARCHAR |
| json | JSON/STRING |

## Example
- Create a table with all supported types in MySQL

```
CREATE TABLE test_db_1.one_pk_no_uk ( 
    f_0 tinyint, 
    f_1 smallint DEFAULT NULL, 
    f_2 mediumint DEFAULT NULL, 
    f_3 int DEFAULT NULL, 
    f_4 bigint DEFAULT NULL, 
    f_5 decimal(10,4) DEFAULT NULL, 
    f_6 float(6,2) DEFAULT NULL, 
    f_7 double(8,3) DEFAULT NULL, 
    f_8 bit(64) DEFAULT NULL,
    f_9 datetime(6) DEFAULT NULL, 
    f_10 time(6) DEFAULT NULL, 
    f_11 date DEFAULT NULL, 
    f_12 year DEFAULT NULL, 
    f_13 timestamp(6) NULL DEFAULT NULL, 
    f_14 char(255) DEFAULT NULL, 
    f_15 varchar(255) DEFAULT NULL, 
    f_16 binary(255) DEFAULT NULL, 
    f_17 varbinary(255) DEFAULT NULL, 
    f_18 tinytext, 
    f_19 text, 
    f_20 mediumtext, 
    f_21 longtext, 
    f_22 tinyblob, 
    f_23 blob, 
    f_24 mediumblob, 
    f_25 longblob, 
    f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, 
    f_27 set('a','b','c','d','e') DEFAULT NULL, 
    f_28 json DEFAULT NULL,
    PRIMARY KEY (f_0) );
```

- The table created in Starrocks by ape_dts
```
    
```

# Supported versions

We've tested on StarRocks 2.5.4 and 3.2.11, refer to [tests](/dt-tests/tests/mysql_to_starrocks/)

For 2.5.4, the stream_load_url should use be_http_port instead of fe_http_port.
# Migrate data from MySQL to StarRocks

# Prerequisites
- [prerequisites](./prerequisites.md)

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Prepare StarRocks instance
- tested versions: 2.5.4 to 3.2.11

```
docker run -itd --name some-starrocks \
-p 9030:9030 \
-p 8030:8030 \
-p 8040:8040 \
"$STARROCKS_IMAGES"
```

# Migrate snapshot data
## Prepare source data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
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
| json | JSON/STRING |

# Supported versions

We've tested on StarRocks 2.5.4 and 3.2.11, refer to [tests](/dt-tests/tests/mysql_to_starrocks/)

For 2.5.4, the stream_load_url should use be_http_port instead of fe_http_port.
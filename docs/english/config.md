# Examples: mysql_to_mysql 
## Snapshot
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*,test_db_3.*
ignore_tbs=
do_events=insert

[router]
db_map=test_db_1:dst_test_db_1
tb_map=test_db_2.one_pk_no_uk_1:dst_test_db_2.dst_one_pk_no_uk_1
col_map=test_db_3.one_pk_no_uk_1.f_0:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_0,test_db_3.one_pk_no_uk_1.f_1:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=1000

[parallelizer]
parallel_type=snapshot
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

## Cdc
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=637309
binlog_filename=mysql-bin.000006
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
heartbeat_interval_secs=1
heartbeat_tb=test_db_1.ape_dts_heartbeat

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*,test_db_3.*
ignore_tbs=
do_events=insert,update,delete

[router]
db_map=test_db_1:dst_test_db_1
tb_map=test_db_2.one_pk_no_uk_1:dst_test_db_2.dst_one_pk_no_uk_1
col_map=test_db_3.one_pk_no_uk_1.f_0:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_0,test_db_3.one_pk_no_uk_1.f_1:dst_test_db_3.dst_one_pk_no_uk_1.dst_f_1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=1000

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# [extractor]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| db_type | source database type| mysql |
| extract_type | snapshot, cdc | snapshot |
| url | database url | mysql://root:123456@127.0.0.1:3307 |

- Different task types may require extra configs, refer to examples in dt-tests/tests

# [sinker]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| db_type | target database type | mysql |
| sink_type | write, check | write |
| url | database url | mysql://root:123456@127.0.0.1:3308 |
| batch_size | number of records written in a batch, 1 for serial | 200 |

- Different task types may require extra configs, refer to examples in dt-tests/tests

# [filter]

| config | meaning | example |
| :-------- | :-------- | :-------- |
| do_dbs | databases to be synced | db_1,db_2*,\`db*&#\` |
| ignore_dbs | databases to be filtered | db_1,db_2*,\`db*&#\` |
| do_tbs | tables to be synced | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` |
| ignore_tbs | tables to be filtered | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` |
| do_events | events to be synced | insert,update,delete |


## Values
- All configs support multiple items, separated by ",", example: do_dbs=db_1,db_2 
- Set to * to match all, example: do_dbs=\*
- Keep empty to match nothing, example: ignore_dbs=
- do_events values: one or more of insert, update, delete

## Priority
- ignore_tbs + ignore_tbs > do_tbs + do_dbs
- If a table matches both ignore configs and do configs, the table will be filtered

## Wildcard

| wildcard | meaning |
| :-------- | :-------- |
| * | matches multiple characters |
| ? | matches 0 or 1 characters |

- Used in: do_dbs，ignore_dbs，do_tbs，ignore_tbs


## Escapes

| database | origin | after escaping |
| :-------- | :-------- | :-------- |
| mysql | db*&# | \`db*&#\` |
| mysql | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg | db*&# | "db*&#" |
| pg | db*&#.tb*$# | "db*&#"."tb*$#" |

- Names should be enclosed in escape characters if there are special characters
- Used in: do_dbs, ignore_dbs, do_tbs, ignore_tbs

# [router]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| db_map | database mapping | db_1:dst_db_1,db_2:dst_db_2 |
| tb_map | table mapping | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2 |
| col_map | column mapping | db_1.tb_1.f_1:dst_db_1.dst_tb_1.dst_f_1,db_1.tb_1.f_2:dst_db_1.dst_tb_1.dst_f_2 |

## Values
- A mapping rule consists of source and target, separated by ":"
- All configs support multiple items, separated by ",", example: db_map=db_1:dst_db_1,db_2:dst_db_2
- If not set, data will be routed to the same databases/tables/columns with source

## Priority
- tb_map > db_map
- col_map only works for column mapping, if a table needs database + table + column mapping, tb_map and db_map must be set, and the database/table mapping rules in col_map must be consistent with tb_map/db_map

## Wildcard
- Not supported

## Escapes
- Same with [filter]

# [pipeline]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| buffer_size | max cached records in memory | 16000 |
| checkpoint_interval_secs | interval to flush logs/statistics/position | 10 |
| max_rps | optional, max synced records in a second | 1000 |

# [parallelizer]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| parallel_type | parallel type | snapshot |
| parallel_size | threads for parallel syncing | 8 |

## parallel_type

| type | strategy | used in | advantages | disadvantages |
| :-------- | :-------- | :-------- |  :-------- | :-------- | 
| snapshot | records in cache are divided into [parallel_size] partitions, each partition will be synced in batches in a separate thread | snapshot tasks for: mysql/pg/mongo | fast |  |
| serial | single thread, one by one | all |  | slow |
| rdb_merge | merge cdc records(insert, update, delete) in cache into insert + delete records，then divide them into [parallel_size] partitions, each partition will be synced in batches in a separate thread| cdc tasks for: mysql/pg | fast | eventual consistency |
| mongo | mongo version of rdb_merge | cdc tasks for: mongo |
| rdb_check | similar to snapshot, but if the source table does not have primary/unique keys, records will be synced in serial | check tasks for: mysql/pg/mongo |
| redis | single thread, batch/serial writing(determined by sinker’s batch_size) | snapshot/cdc tasks for: redis |


# [runtime]
| config | meaning | example |
| :-------- | :-------- | :-------- |
| log_level | level | info/warn/error/debug/trace |
| log4rs_file | log4rs config file | ./log4rs.yaml |
| log_dir | output dir | ./logs |
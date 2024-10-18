# Config details

# Config content

Refer to [task templates](../templates/mysql_to_mysql.md) and [tutorial](../en/tutorial/mysql_to_mysql.md)

# [extractor]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| db_type | source database type| mysql | - |
| extract_type | snapshot, cdc | snapshot | - |
| url | database url | mysql://root:123456@127.0.0.1:3307 | - |
| batch_size | number of extracted records in a batch | 10000 | same as [pipeline] buffer_size |

Since different tasks may require extra configs, please refer to examples in dt-tests/tests for more details.

# [sinker]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| db_type | target database type | mysql | - |
| sink_type | write, check | write | write |
| url | database url | mysql://root:123456@127.0.0.1:3308 | - |
| batch_size | number of records written in a batch, 1 for serial | 200 | 200 |

Since different tasks may require extra configs, please refer to examples in dt-tests/tests for more details.


# [filter]

| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| do_dbs | databases to be synced | db_1,db_2*,\`db*&#\` | - |
| ignore_dbs | databases to be filtered | db_1,db_2*,\`db*&#\` | - |
| do_tbs | tables to be synced | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` | - |
| ignore_tbs | tables to be filtered | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\` | - |
| do_events | events to be synced | insert,update,delete | - |
| do_ddls | ddls to be synced, for mysql cdc tasks | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | - |
| do_structures | structures to be migrated, for mysql/pg structure migration tasks | database,table,constraint,sequence,comment,index | * |
| ignore_cmds | commands to be filtered, for redis cdc tasks | flushall,flushdb | - |


## Values

- All configurations support multiple items, which are separated by ",". Example: do_dbs=db_1,db_2.
- Set to * to match all. Example: do_dbs=\*.
- Keep empty to match nothing. Example: ignore_dbs=.
- do_events take one or more values from **insert**, **update**, and **delete**.

## Priority

- ignore_tbs + ignore_tbs > do_tbs + do_dbs.
- If a table matches both **ignore** configs and **do** configs, the table will be filtered.

## Wildcard

| Wildcard | Meaning |
| :-------- | :-------- |
| * | Matches multiple characters |
| ? | Matches 0 or 1 characters |

Used in: do_dbs, ignore_dbs, do_tbs, and ignore_tbs.

## Escapes

| Database | Before | After |
| :-------- | :-------- | :-------- |
| mysql | db*&# | \`db*&#\` |
| mysql | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg | db*&# | "db*&#" |
| pg | db*&#.tb*$# | "db*&#"."tb*$#" |

Names should be enclosed in escape characters if there are special characters.

Used in: do_dbs, ignore_dbs, do_tbs and ignore_tbs.

# [router]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| db_map | database mapping | db_1:dst_db_1,db_2:dst_db_2 | - |
| tb_map | table mapping | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2 | - |
| col_map | column mapping | db_1.tb_1.f_1:dst_db_1.dst_tb_1.dst_f_1,db_1.tb_1.f_2:dst_db_1.dst_tb_1.dst_f_2 | - |

## Values

- A mapping rule consists of the source and target, which are separated by ":".
- All configurations support multiple items, which are separated by ",". Example: db_map=db_1:dst_db_1,db_2:dst_db_2.
- If not set, data will be routed to the same databases/tables/columns with the source database.

## Priority

- tb_map > db_map.
- col_map only works for column mapping. If a table needs database + table + column mapping, tb_map and db_map must be set, and the database/table mapping rules in col_map must be consistent with those of tb_map/db_map.

## Wildcard

Not supported.

## Escapes

Same with [filter].

# [pipeline]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| buffer_size | max cached records in memory | 16000 | 16000 |
| buffer_memory_mb | [optional] memory limit for buffer, if reached, new records will be blocked even if buffer_size is not reached, 0 means not set | 200 | 0 |
| checkpoint_interval_secs | interval to flush logs/statistics/position | 10 | 10 |
| max_rps | [optional] max synced records in a second| 1000 | - |
| counter_time_window_secs | time window for monitor counters | 10 | same with [pipeline] checkpoint_interval_secs |

# [parallelizer]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| parallel_type | parallel type | snapshot | serial |
| parallel_size | threads for parallel syncing | 8 | 1 |

## parallel_type

|  Type | Strategy | Usage | Advantages | Disadvantages |
| :-------- | :-------- | :-------- |  :-------- | :-------- | 
| snapshot |  Records in cache are divided into [parallel_size] partitions, and each partition will be synced in batches in a separate thread. | snapshot tasks for mysql/pg/mongo | fast |  |
| serial | Single thread, one by one. | all |  | slow |
| rdb_merge | Merge CDC records(insert, update, delete) in cache into insert + delete records，and then divide them into [parallel_size] partitions, each partition synced in batches in a separate thread. | CDC tasks for mysql/pg | fast | eventual consistency |
| mongo | Mongo version of rdb_merge. | CDC tasks for mongo |
| rdb_check | Similar to snapshot. But if the source table does not have primary/unique keys, records will be synced in serial. | check tasks for mysql/pg/mongo |
| redis | Single thread, batch/serial writing(determined by sinker’s batch_size) | snapshot/CDC tasks for redis |


# [runtime]
| Config | Meaning | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| log_level | level | info/warn/error/debug/trace | info |
| log4rs_file | log4rs config file | ./log4rs.yaml | ./log4rs.yaml |
| log_dir | output dir | ./logs | ./logs |
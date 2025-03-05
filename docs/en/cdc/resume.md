# Resume at breakpoint

Task progress will be recorded periodically in position.log.

If task interrupts, you need to restart it manually. By default, it will start from [extractor] configs in task_config.ini.

To avoid syncing duplicate data, the task can resume at the breakpoint in finished.log.

Since resuming depends on position.log, if you have multiple tasks, **you must set up separate log directories for each task**.

## Supported
- MySQL as source
- Postgres as source
- Mongo as source

# Position Info
[position Info](../monitor/position.md)

## MySQL position.log
```
2024-10-10 08:01:09.308022 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000036","next_event_position":773,"gtid_set":"","timestamp":"2024-10-10 08:00:58.000"}
```

## Postgres position.log
```
2024-10-10 09:09:52.260052 | checkpoint_position | {"type":"PgCdc","lsn":"0/406E2C30","timestamp":"2024-10-10 08:12:31.421"}
```

## Mongo position.log 
### op_log
```
2024-10-10 09:17:14.825459 | current_position | {"type":"MongoCdc","resume_token":"","operation_time":1728551829,"timestamp":"2024-10-10 09:17:09.000"}
```

### change_stream
```
2024-10-10 08:46:34.218284 | current_position | {"type":"MongoCdc","resume_token":"{\"_data\":\"8267079350000000012B022C0100296E5A1004B4A9FD2BFD9C44609366CD4CD6A3D98E46645F696400646707935067D762990668C8CE0004\"}","operation_time":1728549712,"timestamp":"2024-10-10 08:41:52.000"}
```

# Configurations

CDC resume configuration is similar to [snapshot task](../snapshot/resume.md), please read first to understand its principles.

Differences:
- MySQL/Postgres position info will load from checkpoint_position in position.log.
- Mongo position info will load from current_position in position.log.

# Example 1

- task_config.ini
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=73351
binlog_filename=mysql-bin.000004

[resumer]
resume_from_log=true
```

- position.log generated before the task was interrupted:
```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

After task restarts, default.log:

```
2024-10-18 07:34:29.702024 - INFO - [1256892] - resume from: {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
2024-10-18 07:34:29.702621 - INFO - [1256892] - MysqlCdcExtractor starts, binlog_filename: mysql-bin.000004, binlog_position: 73685, gtid_enabled: false, gtid_set: , heartbeat_interval_secs: 1, heartbeat_tb: heartbeat_db.ape_dts_heartbeat
```

# Example 2
- task_config.ini
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=73351
binlog_filename=mysql-bin.000004

[resumer]
resume_config_file=./resume.config
```

- ./resume.config (filled in by user)
```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

After task restarts, default.log:

```
2024-10-18 07:40:02.283542 - INFO - [1267442] - resume from: {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
2024-10-18 07:40:02.284100 - INFO - [1267442] - MysqlCdcExtractor starts, binlog_filename: mysql-bin.000004, binlog_position: 73685, gtid_enabled: false, gtid_set: , heartbeat_interval_secs: 1, heartbeat_tb: heartbeat_db.ape_dts_heartbeat
```
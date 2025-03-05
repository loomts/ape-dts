# 断点续传

增量任务进度会定期记录在 position.log。

任务中断后，需要用户手动重启，默认重启后任务将根据 task_config.ini 中 [extractor] 的配置开始同步。

为避免重复同步已完成的数据，可根据 position.log 进行断点续传。

由于断点续传依赖 position.log，故如果你有多个任务，**必须为每个任务设置独立的日志目录**。

## 支持范围
- MySQL 源端
- Postgres 源端
- Mongo 源端

# 进度日志
详细解释可参考 [位点信息](../monitor/position.md)

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

# 配置

增量任务断点续传配置和 [全量任务](../snapshot/resume.md) 类似，请先阅读以了解其原理。

不同点：
- MySQL/Postgres 增量位点信息取自 position.log 中的 checkpoint_position。
- Mongo 增量取位点信息取自 current_position。

# 例子 1（推荐使用）

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

- 任务中断前产生的 position.log
```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

- 任务启动后，default.log 中有如下日志：
```
2024-10-18 07:34:29.702024 - INFO - [1256892] - resume from: {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
2024-10-18 07:34:29.702621 - INFO - [1256892] - MysqlCdcExtractor starts, binlog_filename: mysql-bin.000004, binlog_position: 73685, gtid_enabled: false, gtid_set: , heartbeat_interval_secs: 1, heartbeat_tb: heartbeat_db.ape_dts_heartbeat
```

# 例子 2
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

- ./resume.config（需由用户写入）
```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

- 任务启动后，default.log 中有如下日志：
```
2024-10-18 07:40:02.283542 - INFO - [1267442] - resume from: {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":73685,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
2024-10-18 07:40:02.284100 - INFO - [1267442] - MysqlCdcExtractor starts, binlog_filename: mysql-bin.000004, binlog_position: 73685, gtid_enabled: false, gtid_set: , heartbeat_interval_secs: 1, heartbeat_tb: heartbeat_db.ape_dts_heartbeat
```
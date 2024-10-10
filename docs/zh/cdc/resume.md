# 简介

增量任务进度会定期记录在 position.log。

任务中断后，需要用户手动重启，默认重启后任务将根据 task_config.ini 中 [extractor] 的配置开始同步。

为避免重复同步已完成的数据，可根据 position.log 进行断点续传。

## 支持范围
- mysql 源端
- pg 源端
- mongo 源端

# 进度日志
详细解释可参考 [位点信息](../position.md)

## mysql position.log
```
2024-10-10 08:01:09.308022 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000036","next_event_position":773,"gtid_set":"","timestamp":"2024-10-10 08:00:58.000"}
```

## pg position.log
```
2024-10-10 09:09:52.260052 | checkpoint_position | {"type":"PgCdc","lsn":"0/406E2C30","timestamp":"2024-10-10 08:12:31.421"}
```

## mongo position.log 
### op_log
```
2024-10-10 09:17:14.825459 | current_position | {"type":"MongoCdc","resume_token":"","operation_time":1728551829,"timestamp":"2024-10-10 09:17:09.000"}
```

### change_stream
```
2024-10-10 08:46:34.218284 | current_position | {"type":"MongoCdc","resume_token":"{\"_data\":\"8267079350000000012B022C0100296E5A1004B4A9FD2BFD9C44609366CD4CD6A3D98E46645F696400646707935067D762990668C8CE0004\"}","operation_time":1728549712,"timestamp":"2024-10-10 08:41:52.000"}
```

# 配置

增量任务断点续传配置和 [全量任务](../snapshot/resume.md) 类似，可参考。

不同点：
- mysql/pg 增量位点信息取自 position.log 中的 checkpoint_position。
- mongo 增量取位点信息取自 current_position。

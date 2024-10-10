# 简介

任务进度会定期记录在 position.log / finished.log 中。

任务中断后，需要用户手动重启，默认重启后任务将从头开始同步。

为避免重复同步已完成的数据，可根据 position.log / finished.log 进行断点续传。

## 支持范围
- mysql 源端
- pg 源端
- mongo 源端

# 进度日志
详细解释可参考 [位点信息](../position.md)

## position.log
```
2024-10-10 04:04:08.152044 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"b","order_col":"id","value":"6"}
2024-10-10 04:04:08.152181 | checkpoint_position | {"type":"None"}
```

## finished.log
```
2024-10-10 04:04:07.803422 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"a"}
2024-10-10 04:04:08.844988 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"b"}
```

# 配置
## 从进度日志断点续传
```
[resumer]
resume_from_log=true
resume_log_dir=
```
- resume_log_dir 为可选，默认为当前任务的日志目录。
- 任务重启后，finished.log 中的表将不会被重复同步。
- 正在同步且未完成的表，会根据 position.log 中记录的最新进度，从断点处开始同步。

## 指定进度信息文件
- 除了 resume_from_log，用户也可选择指定进度文件。
```
[resumer]
resume_config_file=./resume.config
```

- resume config 文件内容格式基本和 position.log / finished.log 保持一致，如：
```
| current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"a","order_col":"id","value":"6"}
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"d"}
```

参考测试用例：
- dt-tests/tests/mysql_to_mysql/snapshot/resume_test
- dt-tests/tests/pg_to_pg/snapshot/resume_test
- dt-tests/tests/mongo_to_mongo/snapshot/resume_test
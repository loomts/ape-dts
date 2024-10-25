# 断点续传

任务进度会定期记录在 position.log / finished.log 中。

任务中断后，需要用户手动重启，默认重启后任务将从头开始同步。

为避免重复同步已完成的数据，可根据 position.log / finished.log 进行断点续传。

## 支持范围
- MySQL 源端
- Postgres 源端
- Mongo 源端

# 进度日志
详细解释可参考 [位点信息](../monitor/position.md)

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
- 如果一张表没有 **单一列构成的 主键/唯一键**，则 position.log 中不会产生位点信息，但 finished.log 中会有完成信息。

## 指定进度信息文件
- 除了 resume_from_log，用户也可选择指定进度文件。
```
[resumer]
resume_config_file=./resume.config
```

- resume.config 文件内容格式基本和 position.log / finished.log 保持一致，如：
```
| current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"a","order_col":"id","value":"6"}
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"d"}
```

- 如果同一张表的进度在 position.log 和 resume.config 中都存在，优先使用 position.log。


# 例子
- task_config.ini
```
[resumer]
resume_from_log=true
resume_log_dir=./resume_logs
resume_config_file=./resume.config
```

- ./resume.config
```
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$1"}
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$2"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"5"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_multi_uk","order_col":"f_0","value":"5"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"resume_table_*$4","order_col":"p.k","value":"1"}
```

- ./resume_logs/finished.log
```
2024-04-01 07:08:05.459594 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"in_finished_log_table_*$1"}
2024-04-01 07:08:06.537135 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"in_finished_log_table_*$2"}
```

- ./resume_logs/position.log
```
2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"in_position_log_table_*$1","order_col":"p.k","value":"0"}
2024-03-29 07:02:24.463777 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"in_position_log_table_*$1","order_col":"p.k","value":"1"}
```

- `test_db_@`.`finished_table_*$1`, `test_db_@`.`finished_table_*$2` 在 resume.config 标记为 finished.
- `test_db_@`.`in_finished_log_table_*$1`, `test_db_@`.`in_finished_log_table_*$2` 在 finished.log 中标记为 finished.
- `test_db_1`.`one_pk_no_uk`, `test_db_1`.`one_pk_multi_uk`, `test_db_@`.`resume_table_*$4` 在 resume.config 中有位点信息。
- `test_db_@`.`in_position_log_table_*$1` 在 position.log 中有位点信息。

任务启动后，default.log 中有如下日志：

```
2024-10-18 06:51:10.161794 - INFO - [1180981] - resumer, get resume value, schema: test_db_1, tb: one_pk_multi_uk, col: f_0, result: Some("5")
2024-10-18 06:51:11.193382 - INFO - [1180981] - resumer, get resume value, schema: test_db_1, tb: one_pk_no_uk, col: f_0, result: Some("5")
2024-10-18 06:51:12.135065 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: finished_table_*$1, result: true
2024-10-18 06:51:12.135186 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: finished_table_*$2, result: true
2024-10-18 06:51:12.135227 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: in_finished_log_table_*$1, result: true
2024-10-18 06:51:12.135265 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: in_finished_log_table_*$2, result: true
2024-10-18 06:51:12.268390 - INFO - [1180981] - resumer, get resume value, schema: test_db_@, tb: in_position_log_table_*$1, col: p.k, result: Some("1")
2024-10-18 06:51:13.390645 - INFO - [1180981] - resumer, get resume value, schema: test_db_@, tb: resume_table_*$4, col: p.k, result: Some("1")
```

## 参考测试用例
- dt-tests/tests/mysql_to_mysql/snapshot/resume_test
- dt-tests/tests/pg_to_pg/snapshot/resume_test
- dt-tests/tests/mongo_to_mongo/snapshot/resume_test
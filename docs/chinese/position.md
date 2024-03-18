# 简介

我们把进度信息记录在 position.log 中。位点刷新频率可在 [pipeline] 的 checkpoint_interval_secs 中配置。

# 增量

对于 CDC 任务，因为我们只保证目标库与源库的最终一致性，而不保证源库事务在目标库重放时的完整性，position.log 中记录 current_position 和 checkpoint_position 两份信息。

- current_position：已同步的数据的位点信息，可能处在源库某个大事务 binlog 的中间位置。
- checkpoint_position：已同步的完整的事务的位点信息。

如果任务中断，要使用 position.log 中的位点信息做断点续传。优先使用 checkpoint_position 作为新 CDC 任务的起点，使用 current_position 可能导致 extractor 解析 CDC 数据失败。

## MySQL

可使用 binlog_filename + next_event_position（即 binlog_position）做断点续传。

```
2024-02-28 09:21:53.436763 | checkpoint_position | {"MysqlCdc":{"server_id":"","binlog_filename":"mysql-bin.000054","next_event_position":3301420,"timestamp":"2024-02-28 09:21:51.000 UTC-0000"}}
```

## PG

可使用 lsn 做断点续传。

```
2024-02-28 09:41:01.082135 | checkpoint_position | {"PgCdc":{"lsn":"2/2EBAE0D8","timestamp":"2024-02-28 09:40:47.662 UTC-0000"}}
```

## Mongo

Mongo CDC 任务没有记录 checkpoint_position，您可以：

- 使用 operation_time 做断点续传（对应 extractor 配置 source=op_log）。
- 使用 resume_token 做断点续传（对应 extractor 配置 source=change_stream）。

```
2024-02-28 09:47:25.554048 | current_position | {"MongoCdc":{"resume_token":"","operation_time":1709113643,"timestamp":"2024-02-28 09:47:23.000 UTC-0000"}}
```

```
2024-03-04 09:24:11.898540 | current_position | {"MongoCdc":{"resume_token":"{\"_data\":\"8265E59339000000032B022C0100296E5A1004184FF38FEBC24BF981D8CF6C7AC5D3FE46645F6964006465E593330723C12A0F3BBC2E0004\"}","operation_time":1709544249,"timestamp":"2024-03-04 09:24:09.000 UTC-0000"}}
```


## Redis

位点信息如下，暂不支持断点续传。

```
2024-02-28 09:56:09.924714 | checkpoint_position | {"Redis":{"repl_id":"66f89bb2de0701ecb115f45e46655b366d9fcac8","repl_offset":4850539,"now_db_id":0,"timestamp":""}}
```

# 全量

全量任务如果包含多个库/多张表，则会按照 **先库后表** 排序，**依次同步** 各张表，有且只有一张表处于同步中。

如果表具有单一主键/唯一键，则 extractor 会以此键作为排序列，并从小到大分片拉取；如果表没有排序列，则 extractor 会流式拉取该表所有数据。

## default.log

某张表同步开始/完成，则在 default.log 中记录如下日志：

```
2024-02-28 10:07:35.531681 - INFO - [14778588] - start extracting data from `test_db_1`.`one_pk_no_uk` by slices
2024-02-28 10:07:35.735439 - INFO - [14778588] - end extracting data from `test_db_1`.`one_pk_no_uk`, all count: 9
```

## position.log

如果表数据较多，则会将当前进度信息写入 position.log。

全量进度存储的是当前 **正在同步的表** 的 **最后一条已同步数据** 的排序列的值，该值可用于断点续传。

### MySQL

```
2024-02-28 10:07:35.791465 | current_position | {"RdbSnapshot":{"db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}}
```

### PG

```
2024-02-29 01:16:41.657839 | current_position | {"RdbSnapshot":{"db_type":"pg","schema":"public","tb":"bitbin_table","order_col":"pk","value":"2"}}
```

### Mongo

暂不支持断点续传。

```
2024-02-29 01:22:27.790313 | current_position | {"RdbSnapshot":{"db_type":"mongo","schema":"test_db_2","tb":"tb_1","order_col":"_id","value":"65dfdc512e7b06b6e2b3a3a1"}}
```
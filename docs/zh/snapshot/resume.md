# 简介

如果同步任务异常中断，则需要进行断点续传。

请注意，同步任务本身并不具备自动断点续传的能力。在任务退出后，用户可以在 [position.log](../position.md) 中找到任务退出前的进度信息，并将这些信息更新到 task_config.ini 的 [resumer] 配置。

如果使用更新的 task_config.ini 重启任务，将从断点处开始同步数据。

# 配置

在 [迁移配置](./migration.md) 的基础上添加 [resumer] 配置，目前支持 MySQL 和 PG。

配置中，`rdb_snapshot_positions` 的值为一个经 json 序列化的 map。它的 key 为 库名.表名.排序列，value 为排序列的起始值。

## MySQL

支持转义符 `，参考下例。

```
[resumer]
rdb_snapshot_positions={"test_db_1.no_pk_no_uk.f_0":"5","test_db_1.one_pk_no_uk.f_0":"5","test_db_1.no_pk_one_uk.f_0":"5","test_db_1.no_pk_multi_uk.f_0":"5","test_db_1.one_pk_multi_uk.f_0":"5","`test_db_@`.`resume_table_*$4`.`p.k`":"1"}
```

## PG

支持转义符 "，参考下例。

```
[resumer]
rdb_snapshot_positions={"public.resume_table_2.\"p.k\"":"1","\"test_db_*.*\".\"resume_table_*$5\".\"p.k\"":"1","public.\"resume_table_*$4\".\"p.k\"":"1","public.resume_table_1.pk":"1","public.resume_table_3.f_0":"1"}
```
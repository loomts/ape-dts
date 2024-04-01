# 简介

如果同步任务异常中断，则需要进行断点续传。

请注意，同步任务本身并不具备自动断点续传的能力。在任务退出后，用户可以在 [position.log](../position.md) 中找到任务退出前的进度信息，并将这些信息更新到 task_config.ini 的 [resumer] 配置。

如果使用更新的 task_config.ini 重启任务，将从断点处开始同步数据。

# 配置

在 [迁移配置](./migration.md) 的基础上添加 [resumer] 配置，目前支持 MySQL 和 PG。

`tb_positions` ：为一个经 json 序列化的 map。它的 key 为 库名.表名.排序列，value 为排序列的起始值，配置了此项的表将会从指定起始值同步数据。

`finished_tbs` ：已经完成的表，将不会被同步。

`resume_from_log` ：是否从任务中断前输出的 position.log 和 finished.log 解析出任务进度，并根据该进度自动断点续传。不配置则为 false。

`resume_log_dir` ：指定 position.log 和 finished.log 的位置，不配置则默认为该任务的运行日志输出目录。

参考测试用例：
- dt-tests/tests/mysql_to_mysql/snapshot/resume_test
- dt-tests/tests/pg_to_pg/snapshot/resume_test

## MySQL

支持转义符 `，参考下例。

```
[resumer]
tb_positions={"test_db_1.no_pk_no_uk.f_0":"5","test_db_1.one_pk_no_uk.f_0":"5","test_db_1.no_pk_one_uk.f_0":"5","test_db_1.no_pk_multi_uk.f_0":"5","test_db_1.one_pk_multi_uk.f_0":"5","`test_db_@`.`resume_table_*$4`.`p.k`":"1"}
finished_tbs=`test_db_@`.`finished_table_*$1`,`test_db_@`.`finished_table_*$2`
resume_from_log=true
resume_log_dir=./logs
```

## PG

支持转义符 "，参考下例。

```
[resumer]
tb_positions={"public.resume_table_2.\"p.k\"":"1","\"test_db_*.*\".\"resume_table_*$5\".\"p.k\"":"1","public.\"resume_table_*$4\".\"p.k\"":"1","public.resume_table_1.pk":"1","public.resume_table_3.f_0":"1"}
finished_tbs="test_db_*.*"."finished_table_*$1","test_db_*.*"."finished_table_*$2"
resume_from_log=true
resume_log_dir=./logs
```
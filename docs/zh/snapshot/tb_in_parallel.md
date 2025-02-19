# 全量任务多表并行

如果全量任务涉及多张表，默认情况下，ape-dts 会按照 **先库名后表名** 的方式排序并逐表迁移，同一时刻只有一张表处于迁移中。

如果你的资源（内存，CPU）充足，可以开启多表并行以加快全局速度。

## 配置
- 添加以下配置，ape-dts 会每次迁移 4 张表，如果有某张表迁移完成，ape-dts 会从剩余表中依序选择一张加入迁移任务，确保同时有 4 张表处于迁移中。

```
[runtime]
tb_parallel_size=4
```

## 和 [parallelizer] parallel_size 的区别
- 全量任务中，[parallelizer] 中的配置是针对单张表而言。如以下配置代表每张正在迁移的表会使用 8 个线程并行写入目标端。
- 而 [runtime] 中的 tb_parallel_size 代表的是任务中同时有 4 张表在做迁移。

```
[parallelizer]
parallel_type=snapshot
parallel_size=8
```

## 适用范围
- 全量迁移（源端：MySQL, Postgres, MongoDB）
- 全量校验（源端：MySQL, Postgres, MongoDB）
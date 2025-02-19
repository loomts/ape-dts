# Multiple Tables in Parallel for Snapshot Task

By default, when a snapshot task includes multiple tables, ape-dts migrates tables one at a time in order, sorted by **database name first, then table name**.

If you have sufficient resources (memory, CPU), you can enable parallel table migration to accelerate.

## Configuration
- with following configuration, ape-dts will migrate 4 tables at a time. When any table completes, it will sequentially select another table from the remaining ones to migrate, ensuring that 4 tables are being migrated simultaneously.

```
[runtime]
tb_parallel_size=4
```

## Difference from [parallelizer] parallel_size
- In snapshot tasks, the configuration in [parallelizer] applies to each individual table. For example, the following configuration means each table being migrated will use 8 threads to write to the target in parallel.
- While [runtime] tb_parallel_size means that 4 tables are being migrated simultaneously in the task.

```
[parallelizer]
parallel_type=snapshot
parallel_size=8
```

## Scenarios
- Snapshot migration (Source: MySQL, Postgres, MongoDB)
- Snapshot check (Source: MySQL, Postgres, MongoDB)
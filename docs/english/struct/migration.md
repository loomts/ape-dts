# Introduction

- Database: MySQL, PG.
- Migrated Objects: database(mysql), schema(pg), table, comment, index, sequence(pg), constraints.

# Configurations

```
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=mysql
batch_size=1
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
conflict_policy=interrupt

[filter]
do_dbs=struct_it_mysql2mysql_1
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=serial
parallel_size=1

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs

[pipeline]
checkpoint_interval_secs=10
buffer_size=100
```

## Note

Structure migration is executed serially in a single thread. Notice the following configurations:

```
[extractor]
extract_type=struct

[sinker]
sink_type=struct
batch_size=1

[parallelizer]
parallel_type=serial
parallel_size=1
```

Failure strategy: interrupt(default), ignore.

- interrupt: If a particular migration fails, the entire task will be terminated immediately.

- ignore: If a migration fails, it will not affect the migration of other schemas, and the process will continue. However, the failure will be logged as an error.

```
[sinker]
conflict_policy=interrupt
```

# Phased migration

In a complete data migration process that includes both structure migration and data migration, the task will be divided into three stages in order to accelerate data migration:
1. Migrate table structures + primary/unique keys ( necessities for data migration);
2. Data migration;
3. Migrate indexes + constraints.

Thus, we offer 2 types of filtering:

## Migrate table structures + primary/unique keys
```
[filter]
do_structures=database,table
```

## Migrate indexes and constraints
```
[filter]
do_structures=constraint,index
```
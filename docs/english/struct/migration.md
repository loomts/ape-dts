# Introduction
- Used in: mysql, pg
- Migrate structures for: database(mysql), schema(pg), table, comment, index, sequence(pg), constraints

# Config
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
field_map=

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

## Explain
- Structure migration is executed serially in single thread, specific configs:

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

- Failure strategy: interrupt(default), ignore 

```
[sinker]
conflict_policy=interrupt
```

# Phased migration
- In a task with structure + data migration, in order to accelerate data migration, sometimes the task should be split into 3 steps:
    - 1, Migrate table structures + primary/unique keys, which are necessary for data migration
    - 2, Data migration
    - 3, Migrate indexes + constraints
- Thus, we offer 2 types of filtering:

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
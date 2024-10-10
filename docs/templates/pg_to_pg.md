# struct
```
[extractor]
extract_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
sink_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
conflict_policy=interrupt

[filter]
do_dbs=test_schema
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=
do_structures=

[router]
db_map=
tb_map=
col_map=

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs

[parallelizer]
parallel_type=serial

[pipeline]
checkpoint_interval_secs=10
buffer_size=100
```

# snapshot
```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_schema.a,test_schema.b
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# cdc
```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test

[filter]
do_dbs=
do_events=insert,update,delete
ignore_dbs=
ignore_tbs=
do_tbs=test_schema.a,test_schema.b

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
batch_size=200

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# struct check
```
[extractor]
db_type=pg
extract_type=struct
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=pg
sink_type=check
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_schema.*
ignore_tbs=
do_events=

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# data check
```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000

[sinker]
db_type=pg
sink_type=check
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_schema.a,test_schema.b
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# data revise
```
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./logs/check
batch_size=200

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_schema.a,test_schema.b
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# data review
```
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./logs/origin_check_log
batch_size=200

[sinker]
db_type=pg
sink_type=check
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_schema.a,test_schema.b
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# cdc to sqls
```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test

[filter]
do_dbs=test_schema
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# cdc to reverse sqls
```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test

[filter]
do_dbs=test_schema
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql
reverse=true

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```
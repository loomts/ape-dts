# snapshot
```
[extractor]
db_type=redis
extract_type=snapshot
repl_port=10008
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=

[sinker]
db_type=redis
sink_type=write
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# snapshot + cdc
```
[extractor]
db_type=redis
extract_type=cdc
repl_id=
now_db_id=0
repl_port=10008
repl_offset=0
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=
ignore_cmds=flushall

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# cdc
```
[extractor]
db_type=redis
extract_type=cdc
repl_id=e955b7b98cf5d635c936661458c3467f3ec32c45
now_db_id=3
repl_port=10008
repl_offset=4705381
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=
ignore_cmds=flushall

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```
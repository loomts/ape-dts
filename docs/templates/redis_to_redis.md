# Redis -> Redis templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# Snapshot
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

- [extractor]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| repl_port | ape_dts uses PSYNC to pull Redis data, repl_port is used in "replconf listening-port [port]" command | 10008 | 10008 |

# Snapshot + CDC
```
[extractor]
db_type=redis
extract_type=cdc
repl_port=10008
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
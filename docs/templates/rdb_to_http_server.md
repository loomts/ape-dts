# MySQL/Postgres -> ape_dts(HTTP Server) templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# MySQL Snapshot
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert

[sinker]
sink_type=dummy

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
pipeline_type=http_server
http_host=0.0.0.0
http_port=10231
with_field_defs=true

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- [pipeline]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| http_host | the host to bind when starting http server | 127.0.0.1 | 0.0.0.0 |
| http_port | the port to bind when starting http server | 10231 | 10231 |
| with_field_defs | when sending data to clients in avro format, include the definitions of data fields or not | true | true |

# MySQL CDC
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=0
binlog_filename=
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=*.*
ignore_tbs=
do_events=insert,update,delete
do_ddls=*

[sinker]
sink_type=dummy

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
pipeline_type=http_server
http_host=0.0.0.0
http_port=10231
with_field_defs=true

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# Posgres Snapshot

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000
```

# Postgres CDC

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test
```
# snapshot
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
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[router]
db_map=
tb_map=
col_map=
topic_map=*.*:test,test_db_2.*:test2,test_db_2.tb_1:test3

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# cdc
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
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[router]
db_map=
tb_map=
col_map=
topic_map=*.*:test

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```
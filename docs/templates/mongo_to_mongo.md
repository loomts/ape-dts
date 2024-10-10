# snapshot
```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
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

# cdc, by op_log
```
[extractor]
db_type=mongo
extract_type=cdc
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
source=op_log
start_timestamp=1728525445

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mongo
sink_type=write
batch_size=200
url=mongodb://ape_dts:123456@127.0.0.1:27018

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# cdc, by change_stream
```
[extractor]
db_type=mongo
extract_type=cdc
resume_token={"_data":"826707373B000000012B022C0100296E5A1004B4A9FD2BFD9C44609366CD4CD6A3D98E46645F696400646707373B22E3B8A398F7FB340004"}
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
source=change_stream

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mongo
sink_type=write
batch_size=200
url=mongodb://ape_dts:123456@127.0.0.1:27018

[router]
tb_map=
col_map=
db_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=mongo
parallel_size=8

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# check
```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[sinker]
db_type=mongo
sink_type=check
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
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

# revise
```
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
check_log_dir=./logs/origin_check_log
batch_size=200

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# review
```
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
check_log_dir=./logs/origin_check_log
batch_size=200

[sinker]
db_type=mongo
sink_type=check
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
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
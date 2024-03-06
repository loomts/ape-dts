# Introduction
- Cdc task calculates delay by position, for example, mysql cdc tasks use the synced source binlog offset as position
- The position should be consistent with the source database if the cdc task catches up, and the timestamp of the position(if any) should be current time
- But if the source database has not been updated for a long time, or there are updates but the updated tables are not subscribed by cdc task, then the task position won't change, and it is considered as a delay
- We can pre-create a heartbeat table in the source database and update the table periodically by cdc task to push the task position forward
- Heartbeat if optional

# Config
- mysql/pg/mongo, refer to:
    - dt-tests/tests/mysql_to_mysql/cdc/heartbeat_test
    - dt-tests/tests/pg_to_pg/cdc/heartbeat_test
    - dt-tests/tests/mongo_to_mongo/cdc/heartbeat_test

```
[extractor]
heartbeat_interval_secs=10
heartbeat_tb=test_db_1.ape_dts_heartbeat

[filter]
ignore_tbs=test_db_1.ape_dts_heartbeat
```

- redis，refer to: dt-tests/tests/redis_to_redis/cdc/heartbeat_test
```
[extractor]
heartbeat_interval_secs=10
heartbeat_key=5.ape_dts_heartbeat_key
```

# Heartbeat table
- mysql
```
CREATE TABLE IF NOT EXISTS `{}`.`{}`(
    server_id INT UNSIGNED,
    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    received_binlog_filename VARCHAR(255),
    received_next_event_position INT UNSIGNED,
    received_timestamp VARCHAR(255),
    flushed_binlog_filename VARCHAR(255),
    flushed_next_event_position INT UNSIGNED,
    flushed_timestamp VARCHAR(255),
    PRIMARY KEY(server_id)
)
```

- pg
```
CREATE TABLE IF NOT EXISTS "{}"."{}"(
    slot_name character varying(64) not null,
    update_timestamp timestamp without time zone default (now() at time zone 'utc'),
    received_lsn character varying(64),
    received_timestamp character varying(64),
    flushed_lsn character varying(64),
    flushed_timestamp character varying(64),
    primary key(slot_name)
)
```

- Database & table should keep the same with heartbeat_tb in task_config.ini
- No need to pre-create anything for mongo & redis
- Keep heartbeat_tb empty if not needed
- If heartbeat_tb configured but table not created, cdc task will try to create it, which needs the extractor account to have create privileges
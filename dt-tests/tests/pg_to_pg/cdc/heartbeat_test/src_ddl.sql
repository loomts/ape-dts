DROP SCHEMA IF EXISTS test_db_1 CASCADE;

CREATE SCHEMA test_db_1;

CREATE TABLE test_db_1.ape_dts_heartbeat(
    slot_name character varying(64) not null,
    update_timestamp timestamp without time zone default (now() at time zone 'utc'),
    received_lsn character varying(64),
    received_timestamp character varying(64),
    flushed_lsn character varying(64),
    flushed_timestamp character varying(64),
    primary key(slot_name)
);
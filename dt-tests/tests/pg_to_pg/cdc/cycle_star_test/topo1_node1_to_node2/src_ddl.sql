DROP SCHEMA IF EXISTS twoway_test_db_1 CASCADE;

CREATE SCHEMA IF NOT EXISTS twoway_test_db_1;

CREATE TABLE IF NOT EXISTS twoway_test_db_1.default_table(pk integer, val numeric(20,8), created_at timestamp, created_at_tz timestamptz, ctime time , ctime_tz timetz , cdate date , cmoney money , cbits bit(3) , csmallint smallint , cinteger integer , cbigint bigint , creal real , cbool bool , cfloat8 float8 , cnumeric numeric(6,2) , cvarchar varchar(5) , cbox box , ccircle circle , cinterval interval , cline line , clseg lseg , cpath path , cpoint point , cpolygon polygon , cchar char , ctext text , cjson json , cxml xml , cuuid uuid , cvarbit varbit(3) , cinet inet , ccidr cidr , cmacaddr macaddr , PRIMARY KEY(pk));

TRUNCATE TABLE twoway_test_db_1.default_table;

DROP SCHEMA IF EXISTS ape_trans_pg CASCADE;
CREATE SCHEMA ape_trans_pg;

CREATE TABLE ape_trans_pg.topo1 (
  data_origin_node varchar(255) NOT NULL,
  src_node varchar(255) NOT NULL,
  dst_node varchar(255) NOT NULL,
  n bigint DEFAULT NULL,
  PRIMARY KEY (data_origin_node, src_node, dst_node)
);

SELECT pg_drop_replication_slot('ape_test1') FROM pg_replication_slots WHERE slot_name = 'ape_test1';
SELECT * FROM pg_create_logical_replication_slot('ape_test1', 'pgoutput');

SELECT pg_drop_replication_slot('ape_test2') FROM pg_replication_slots WHERE slot_name = 'ape_test2';
SELECT * FROM pg_create_logical_replication_slot('ape_test2', 'pgoutput');
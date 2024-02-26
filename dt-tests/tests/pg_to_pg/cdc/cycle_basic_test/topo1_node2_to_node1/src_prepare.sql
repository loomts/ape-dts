CREATE SCHEMA IF NOT EXISTS twoway_test_db_1;

CREATE TABLE IF NOT EXISTS twoway_test_db_1.default_table(pk integer, val numeric(20,8), created_at timestamp, created_at_tz timestamptz, ctime time , ctime_tz timetz , cdate date , cmoney money , cbits bit(3) , csmallint smallint , cinteger integer , cbigint bigint , creal real , cbool bool , cfloat8 float8 , cnumeric numeric(6,2) , cvarchar varchar(5) , cbox box , ccircle circle , cinterval interval , cline line , clseg lseg , cpath path , cpoint point , cpolygon polygon , cchar char , ctext text , cjson json , cxml xml , cuuid uuid , cvarbit varbit(3) , cinet inet , ccidr cidr , cmacaddr macaddr , PRIMARY KEY(pk));

DELETE FROM twoway_test_db_1.default_table;

DROP SCHEMA IF EXISTS ape_trans_pg CASCADE;
CREATE SCHEMA ape_trans_pg;

DROP PUBLICATION IF EXISTS apecloud_migrate_pub_for_all_tables;
CREATE PUBLICATION apecloud_migrate_pub_for_all_tables FOR ALL TABLES;

SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
SELECT * FROM pg_create_logical_replication_slot('ape_test', 'pgoutput');
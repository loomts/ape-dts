# setup test envrionments

## postgres

### source

```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d postgis/postgis:latest

docker exec -it some-postgres-1 bash
login: `psql -h 127.0.0.1 -U postgres -d postgres-p 5432 -W`
run `ALTER SYSTEM SET wal_level = logical;` and restart
```

### source ddl tests
```
CREATE TABLE public.ape_dts_ddl_command
(
    ddl_text text COLLATE pg_catalog."default",
   id bigserial primary key,
   event text COLLATE pg_catalog."default",
   tag text COLLATE pg_catalog."default",
   username character varying COLLATE pg_catalog."default",
   database character varying COLLATE pg_catalog."default",
   schema character varying COLLATE pg_catalog."default",
   object_type character varying COLLATE pg_catalog."default",
   object_name character varying COLLATE pg_catalog."default",
   client_address character varying COLLATE pg_catalog."default",
   client_port integer,
   event_time timestamp with time zone,
   txid_current character varying(128) COLLATE pg_catalog."default",
   message text COLLATE pg_catalog."default"
);
```

```
CREATE FUNCTION public.ape_dts_capture_ddl()
    RETURNS event_trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
  declare ddl_text text;
  declare max_rows int := 10000;
  declare current_rows int;
  declare pg_version_95 int := 90500;
  declare pg_version_10 int := 100000;
  declare current_version int;
  declare object_id varchar;
  declare alter_table varchar;
  declare record_object record;
  declare message text;
  declare pub RECORD;
begin

  select current_query() into ddl_text;

  if TG_TAG = 'CREATE TABLE' then -- ALTER TABLE schema.TABLE REPLICA IDENTITY FULL;
    show server_version_num into current_version;
    if current_version >= pg_version_95 then
      for record_object in (select * from pg_event_trigger_ddl_commands()) loop
        if record_object.command_tag = 'CREATE TABLE' then
          object_id := record_object.object_identity;
        end if;
      end loop;
    else
      select btrim(substring(ddl_text from '[ \t\r\n\v\f]*[c|C][r|R][e|E][a|A][t|T][e|E][ \t\r\n\v\f]*.*[ \t\r\n\v\f]*[t|T][a|A][b|B][l|L][e|E][ \t\r\n\v\f]+(.*)\(.*'),' \t\r\n\v\f') into object_id;
    end if;
    if object_id = '' or object_id is null then
      message := 'CREATE TABLE, but ddl_text=' || ddl_text || ', current_query=' || current_query();
    end if;
    if current_version >= pg_version_10 then
      for pub in (select * from pg_publication where pubname like 'ape_dts_%') loop
        raise notice 'pubname=%',pub.pubname;
        BEGIN
          execute 'alter publication ' || pub.pubname || ' add table ' || object_id;
        EXCEPTION WHEN OTHERS THEN
        END;
      end loop;
    end if;
  end if;

  insert into public.ape_dts_ddl_command(id,event,tag,username,database,schema,object_type,object_name,client_address,client_port,event_time,ddl_text,txid_current,message)
  values (default,TG_EVENT,TG_TAG,current_user,current_database(),current_schema,'','',inet_client_addr(),inet_client_port(),current_timestamp,ddl_text,cast(TXID_CURRENT() as varchar(16)),message);

  select count(id) into current_rows from public.ape_dts_ddl_command;
  if current_rows > max_rows then
    delete from public.ape_dts_ddl_command where id in (select min(id) from public.ape_dts_ddl_command);
  end if;
end
$BODY$;
```

```
ALTER FUNCTION public.ape_dts_capture_ddl() OWNER TO postgres;

CREATE EVENT TRIGGER ape_dts_intercept_ddl ON ddl_command_end EXECUTE PROCEDURE public.ape_dts_capture_ddl();

CREATE PUBLICATION ape_dts_publication_for_all_tables FOR ALL tables;
```

### target

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d postgis/postgis:latest
```

- create a test db for EUC_CN in both source and target

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

## mysql

### source

```
docker run -d --name some-mysql-1 \
--platform linux/x86_64 \
-it \
-p 3307:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:5.7.40 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --default_time_zone=+08:00
```

### target

```
docker run -d --name some-mysql-2 \
--platform linux/x86_64 \
-it \
-p 3308:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:8.0.31 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --binlog-transaction-compression \
 --binlog_rows_query_log_events=ON \
 --default_authentication_plugin=mysql_native_password \
 --default_time_zone="+07:00"
```

## mongo

### source

- 1. Create a Docker network:

```
docker network create mongo-network
```

- 2. Start 3 MongoDB containers for the replica set members:

```
docker run -d --name mongo1 --network mongo-network -p 9042:9042 mongo --replSet rs0  --port 9042
docker run -d --name mongo2 --network mongo-network -p 9142:9142 mongo --replSet rs0  --port 9142
docker run -d --name mongo3 --network mongo-network -p 9242:9242 mongo --replSet rs0  --port 9242
```

- 3. Connect to the primary (mongo1) and initialize the replica set:

```
docker exec -it mongo1 bash
mongosh --host localhost --port 9042
> config = {"_id" : "rs0", "members" : [{"_id" : 0,"host" : "mongo1:9042"},{"_id" : 1,"host" : "mongo2:9142"},{"_id" : 2,"host" : "mongo3:9242"}]}
> rs.initiate(config)
> rs.status()
```

- 4. Enable authentication on the admin database:

```
> use admin
> db.createUser({user: "ape_dts", pwd: "123456", roles: ["root"]})
```

- 5. Update /etc/hosts on Mac

```
127.0.0.1 mongo1 mongo2 mongo3
```

- 6. Test the connection from Mac

```
mongo "mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0"
```

### target

```
docker run -d --name dst-mongo \
	-e MONGO_INITDB_ROOT_USERNAME=ape_dts \
	-e MONGO_INITDB_ROOT_PASSWORD=123456 \
  -p 27018:27017 \
	mongo
```

## redis
### images of redis versions
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8

### source

```
docker run --name some-redis-1 \
-p 6380:6379 \
-d redis redis-server \
--requirepass 123456 \
--save 60 1 \
--loglevel warning
```

### target

```
docker run --name some-redis-2 \
-p 6381:6379 \
-d redis redis-server \
--requirepass 123456 \
--save 60 1 \
--loglevel warning
```

## StarRocks
### target
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \ 
  -itd starrocks.docker.scarf.sh/starrocks/allin1-ubuntu
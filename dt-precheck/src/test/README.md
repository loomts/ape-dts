## postgres
- source
```
docker run --name some-postgres-1 \
-p 5431:5432 \
-e POSTGRES_PASSWORD=123456 \
-d postgis/postgis:14-3.3 

run `ALTER SYSTEM SET wal_level = logical;` and restart
```


- target
```
docker run --name some-postgres-2 \
-p 5430:5432 \
-e POSTGRES_PASSWORD=123456 \
-d postgis/postgis:14-3.3
```

## mysql
- source
```
docker run -d --name some-mysql-1 \
-it \
-p 3306:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:8.0.31 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
```

- target
```
docker run -d --name some-mysql-1 \
-it \
-p 3307:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:8.0.31 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
```

## input the database infos into '**/.env' file
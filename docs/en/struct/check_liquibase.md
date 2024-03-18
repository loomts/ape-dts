# Introduction

Liquibase is an open-source software used for managing database schema changes. You can learn more about it by visiting the [Liquibase official website](https://www.liquibase.org/) and exploring the [GitHub repository](https://github.com/liquibase/liquibase).

Since it provides comparisons for database structures, we made a docker image(apecloud/ape-dts-structure-checker:0.0.1) based on Liquibase, which can be used for check tasks after structure migration. Refer to code changes in this [GitHub repository](https://github.com/qianyiwen2019/liquibase/tree/ape_diff_tool).

# Examples

## MySQL
```
docker run \
-e URL="jdbc:mysql://host.docker.internal:3308/test_db_1?useSSL=false" \
-e USERNAME=root \
-e PASSWORD=123456 \
-e REFERENCE_URL="jdbc:mysql://host.docker.internal:3307/test_db_1?useSSL=false" \
-e REFERENCE_USERNAME=root \
-e REFERENCE_PASSWORD=123456 \
apecloud/ape-dts-structure-checker:0.0.1
```

## PG
```
docker run \
-e URL="jdbc:postgresql://host.docker.internal:5438/postgres?currentSchema=struct_check_test_1" \
-e USERNAME=postgres \
-e PASSWORD=postgres \
-e REFERENCE_URL="jdbc:postgresql://host.docker.internal:5437/postgres?currentSchema=struct_check_test_1" \
-e REFERENCE_USERNAME=postgres \
-e REFERENCE_PASSWORD=postgres \
apecloud/ape-dts-structure-checker:0.0.1
```

# Parameters

- URL: Target database url.
- USERNAME: Target username.
- PASSWORD: Target password.
- REFERENCE_URL: Source database url.
- REFERENCE_USERNAME: Source username.
- REFERENCE_PASSWORD: Source password.

# Results

```
Compared Schemas: test_db_1
Product Name: EQUAL
Product Version: EQUAL
Missing Catalog(s): NONE
Unexpected Catalog(s): NONE
Changed Catalog(s): NONE
Missing Column(s): 
     test_db_1.ape_dts_heartbeat.flushed_binlog_filename
     test_db_1.ape_dts_heartbeat.flushed_next_event_position
     test_db_1.ape_dts_heartbeat.flushed_timestamp
Unexpected Column(s): 
     test_db_1.col_has_special_character_table.col"1
Changed Column(s): NONE
Missing Foreign Key(s): NONE
Unexpected Foreign Key(s): NONE
Changed Foreign Key(s): NONE
Missing Index(s): 
     PRIMARY UNIQUE  ON test_db_1.ape_dts_heartbeat(server_id)
Unexpected Index(s): 
     PRIMARY UNIQUE  ON test_db_1.col_has_special_character_table(p:k)
     PRIMARY UNIQUE  ON test_db_1.numeric_table(f_0)
     PRIMARY UNIQUE  ON test_db_1.one_pk_multi_uk(f_0)
     PRIMARY UNIQUE  ON test_db_1.one_pk_no_uk(f_0)
     uk_1 UNIQUE  ON test_db_1.no_pk_multi_uk(f_1, f_2)
     uk_1 UNIQUE  ON test_db_1.no_pk_one_uk(f_1, f_2)
     uk_1 UNIQUE  ON test_db_1.one_pk_multi_uk(f_1, f_2)
     uk_2 UNIQUE  ON test_db_1.no_pk_multi_uk(f_3, f_4, f_5)
     uk_2 UNIQUE  ON test_db_1.one_pk_multi_uk(f_3, f_4, f_5)
     uk_3 UNIQUE  ON test_db_1.no_pk_multi_uk(f_6, f_7, f_8)
     uk_3 UNIQUE  ON test_db_1.one_pk_multi_uk(f_6, f_7, f_8)
Changed Index(s): NONE
Missing Primary Key(s): 
     PRIMARY on test_db_1.ape_dts_heartbeat(server_id)
Unexpected Primary Key(s): 
     PRIMARY on test_db_1.col_has_special_character_table(p:k)
     PRIMARY on test_db_1.numeric_table(f_0)
     PRIMARY on test_db_1.one_pk_multi_uk(f_0)
     PRIMARY on test_db_1.one_pk_no_uk(f_0)
Changed Primary Key(s): NONE
Missing Sequence(s): NONE
Unexpected Sequence(s): NONE
Changed Sequence(s): NONE
Missing Table(s): 
     ape_dts_heartbeat
Unexpected Table(s): 
     col_has_special_character_table
     no_pk_multi_uk
     no_pk_no_uk
     no_pk_one_uk
     numeric_table
     one_pk_multi_uk
     one_pk_no_uk
Changed Table(s): NONE
Missing Unique Constraint(s): NONE
Unexpected Unique Constraint(s): 
     uk_1 on no_pk_multi_uk(f_1, f_2)
     uk_1 on no_pk_one_uk(f_1, f_2)
     uk_1 on one_pk_multi_uk(f_1, f_2)
     uk_2 on no_pk_multi_uk(f_3, f_4, f_5)
     uk_2 on one_pk_multi_uk(f_3, f_4, f_5)
     uk_3 on no_pk_multi_uk(f_6, f_7, f_8)
     uk_3 on one_pk_multi_uk(f_6, f_7, f_8)
Changed Unique Constraint(s): NONE
Missing View(s): NONE
Unexpected View(s): NONE
Changed View(s): NONE
Liquibase command 'diff' was executed successfully.
```
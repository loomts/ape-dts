DROP DATABASE IF EXISTS dst_test_db_1;
DROP DATABASE IF EXISTS dst_test_db_2;
DROP DATABASE IF EXISTS test_db_2;
DROP DATABASE IF EXISTS dst_test_db_3;
DROP DATABASE IF EXISTS test_db_3;

CREATE DATABASE dst_test_db_1;
CREATE DATABASE dst_test_db_2;
CREATE DATABASE test_db_2;
CREATE DATABASE dst_test_db_3;
CREATE DATABASE test_db_3;

-- db map
CREATE TABLE dst_test_db_1.one_pk_no_uk_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 
CREATE TABLE dst_test_db_1.one_pk_no_uk_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

-- tb map
CREATE TABLE dst_test_db_2.dst_one_pk_no_uk_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 
-- no map
CREATE TABLE test_db_2.one_pk_no_uk_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

-- col map
CREATE TABLE dst_test_db_3.dst_one_pk_no_uk_1 ( dst_f_0 tinyint, dst_f_1 smallint DEFAULT NULL, PRIMARY KEY (dst_f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 
-- no map
CREATE TABLE test_db_3.one_pk_no_uk_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

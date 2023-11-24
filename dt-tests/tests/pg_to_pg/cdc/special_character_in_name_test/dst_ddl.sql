DROP SCHEMA IF EXISTS "test_db_*.*" CASCADE;
DROP SCHEMA IF EXISTS "test_db_&.&" CASCADE;
DROP SCHEMA IF EXISTS "test_db_^.^" CASCADE;
DROP SCHEMA IF EXISTS "test_db_@.@" CASCADE;

DROP SCHEMA IF EXISTS "*.*_test_db" CASCADE;
DROP SCHEMA IF EXISTS "&.&_test_db" CASCADE;
DROP SCHEMA IF EXISTS "^.^_test_db" CASCADE;
DROP SCHEMA IF EXISTS "@.@_test_db" CASCADE;

CREATE SCHEMA "test_db_*.*";
CREATE SCHEMA "test_db_&.&";
CREATE SCHEMA "test_db_^.^";
CREATE SCHEMA "test_db_@.@";

CREATE SCHEMA "*.*_test_db";
CREATE SCHEMA "&.&_test_db";
CREATE SCHEMA "^.^_test_db";
CREATE SCHEMA "@.@_test_db";

CREATE TABLE "test_db_*.*"."one_pk_no_uk_1_*.*" ( "f_0_*.*" SERIAL, "f_1_*.*" SMALLINT,  PRIMARY KEY ("f_0_*.*") ); 
CREATE TABLE "test_db_*.*"."one_pk_no_uk_2_*.*" ( "f_0_*.*" SERIAL, "f_1_*.*" SMALLINT,  PRIMARY KEY ("f_0_*.*") ); 

CREATE TABLE "test_db_&.&"."one_pk_no_uk_1_&.&" ( "f_0_&.&" SERIAL, "f_1_&.&" SMALLINT,  PRIMARY KEY ("f_0_&.&") ); 
CREATE TABLE "test_db_&.&"."one_pk_no_uk_2_&.&" ( "f_0_&.&" SERIAL, "f_1_&.&" SMALLINT,  PRIMARY KEY ("f_0_&.&") ); 

CREATE TABLE "test_db_^.^"."one_pk_no_uk_1_^.^" ( "f_0_^.^" SERIAL, "f_1_^.^" SMALLINT,  PRIMARY KEY ("f_0_^.^") ); 
CREATE TABLE "test_db_^.^"."one_pk_no_uk_2_^.^" ( "f_0_^.^" SERIAL, "f_1_^.^" SMALLINT,  PRIMARY KEY ("f_0_^.^") ); 

CREATE TABLE "test_db_@.@"."one_pk_no_uk_1_@.@" ( "f_0_@.@" SERIAL, "f_1_@.@" SMALLINT,  PRIMARY KEY ("f_0_@.@") ); 
CREATE TABLE "test_db_@.@"."one_pk_no_uk_2_@.@" ( "f_0_@.@" SERIAL, "f_1_@.@" SMALLINT,  PRIMARY KEY ("f_0_@.@") ); 

CREATE TABLE "*.*_test_db"."one_pk_no_uk_1_*.*" ( "f_0_*.*" SERIAL, "f_1_*.*" SMALLINT,  PRIMARY KEY ("f_0_*.*") ); 
CREATE TABLE "*.*_test_db"."one_pk_no_uk_2_*.*" ( "f_0_*.*" SERIAL, "f_1_*.*" SMALLINT,  PRIMARY KEY ("f_0_*.*") ); 

CREATE TABLE "&.&_test_db"."one_pk_no_uk_1_&.&" ( "f_0_&.&" SERIAL, "f_1_&.&" SMALLINT,  PRIMARY KEY ("f_0_&.&") ); 
CREATE TABLE "&.&_test_db"."one_pk_no_uk_2_&.&" ( "f_0_&.&" SERIAL, "f_1_&.&" SMALLINT,  PRIMARY KEY ("f_0_&.&") ); 

CREATE TABLE "^.^_test_db"."one_pk_no_uk_1_^.^" ( "f_0_^.^" SERIAL, "f_1_^.^" SMALLINT,  PRIMARY KEY ("f_0_^.^") ); 
CREATE TABLE "^.^_test_db"."one_pk_no_uk_2_^.^" ( "f_0_^.^" SERIAL, "f_1_^.^" SMALLINT,  PRIMARY KEY ("f_0_^.^") ); 

CREATE TABLE "@.@_test_db"."one_pk_no_uk_1_@.@" ( "f_0_@.@" SERIAL, "f_1_@.@" SMALLINT,  PRIMARY KEY ("f_0_@.@") ); 
CREATE TABLE "@.@_test_db"."one_pk_no_uk_2_@.@" ( "f_0_@.@" SERIAL, "f_1_@.@" SMALLINT,  PRIMARY KEY ("f_0_@.@") ); 
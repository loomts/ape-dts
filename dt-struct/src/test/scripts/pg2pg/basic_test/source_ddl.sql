create schema struct_it_pg2pg_1;

-- full column types:
CREATE TABLE struct_it_pg2pg_1.full_column_type (id SERIAL PRIMARY KEY,varchar_col VARCHAR(255) NOT NULL,char_col CHAR(10),text_col TEXT,boolean_col BOOLEAN,smallint_col SMALLINT,integer_col INTEGER,bigint_col BIGINT,decimal_col DECIMAL(10, 2),numeric_col NUMERIC(10, 2),real_col REAL,double_precision_col DOUBLE PRECISION,date_col DATE,time_col TIME,timestamp_col TIMESTAMP,interval_col INTERVAL,bytea_col BYTEA,uuid_col UUID,xml_col XML,json_col JSON,jsonb_col JSONB,point_col POINT,line_col LINE,lseg_col LSEG,box_col BOX,path_col PATH,polygon_col POLYGON,circle_col CIRCLE);

-- full check types(without fk and exclude):
CREATE TABLE struct_it_pg2pg_1.full_constraint_type (id SERIAL PRIMARY KEY,varchar_col VARCHAR(255) NOT NULL,unique_col VARCHAR(255) UNIQUE,not_null_col VARCHAR(255) NOT NULL,check_col VARCHAR(255) CHECK (char_length(check_col) > 3));

-- full index types:
CREATE TABLE struct_it_pg2pg_1.full_index_type (id SERIAL PRIMARY KEY,unique_col VARCHAR(255) NOT NULL,index_col VARCHAR(255),fulltext_col TSVECTOR,spatial_col POINT NOT NULL,simple_index_col VARCHAR(255),composite_index_col1 VARCHAR(255),composite_index_col2 VARCHAR(255),composite_index_col3 VARCHAR(255));

CREATE UNIQUE INDEX unique_index ON struct_it_pg2pg_1.full_index_type (unique_col);

CREATE INDEX index_index ON struct_it_pg2pg_1.full_index_type (index_col);

CREATE INDEX fulltext_index ON struct_it_pg2pg_1.full_index_type USING gin(fulltext_col);

CREATE INDEX spatial_index ON struct_it_pg2pg_1.full_index_type USING gist(spatial_col);

CREATE INDEX simple_index ON struct_it_pg2pg_1.full_index_type (simple_index_col);

CREATE INDEX composite_index ON struct_it_pg2pg_1.full_index_type (composite_index_col1, composite_index_col2, composite_index_col3);

-- comment:
COMMENT ON TABLE struct_it_pg2pg_1.full_column_type IS 'This is an example table.';

COMMENT ON COLUMN struct_it_pg2pg_1.full_column_type.id IS 'This is the primary key column.';
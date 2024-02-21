drop schema if exists struct_it_pg2pg_1 CASCADE;

create schema struct_it_pg2pg_1;

-- all index types:
CREATE TABLE struct_it_pg2pg_1.full_index_type (id SERIAL PRIMARY KEY,unique_col VARCHAR(255) NOT NULL,index_col VARCHAR(255),fulltext_col TSVECTOR,spatial_col POINT NOT NULL,simple_index_col VARCHAR(255),composite_index_col1 VARCHAR(255),composite_index_col2 VARCHAR(255),composite_index_col3 VARCHAR(255),check_col VARCHAR(255));

-- indexes
CREATE UNIQUE INDEX unique_index ON struct_it_pg2pg_1.full_index_type (unique_col);

-- table comments:
COMMENT ON TABLE struct_it_pg2pg_1.full_index_type IS 'Comment on full_index_type.';

-- column comments:
COMMENT ON COLUMN struct_it_pg2pg_1.full_index_type.id IS 'Comment on full_index_type.id.';

-- foreign key constraints:
CREATE TABLE struct_it_pg2pg_1.foreign_key_parent (pk SERIAL, parent_col_1 INTEGER UNIQUE, parent_col_2 INTEGER UNIQUE, PRIMARY KEY(pk));
CREATE TABLE struct_it_pg2pg_1.foreign_key_child (pk SERIAL, child_col_1 INTEGER UNIQUE, child_col_2 INTEGER UNIQUE, PRIMARY KEY(pk));

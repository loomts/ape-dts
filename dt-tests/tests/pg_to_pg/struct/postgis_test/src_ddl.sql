CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS isn;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgis;

DROP SCHEMA IF EXISTS struct_it_pg2pg_postgis CASCADE;
CREATE SCHEMA struct_it_pg2pg_postgis;

-- refer: https://postgis.net/docs/using_postgis_dbmanagement.html

------------------------------------- Geography ---------------------------------------
CREATE TABLE struct_it_pg2pg_postgis.geography_1(pk serial PRIMARY KEY, value geography);

------------------------------------- Geography, POINT
-- Create a table with 2D POINT geography with the default SRID 4326 (WGS84 long/lat):
CREATE TABLE struct_it_pg2pg_postgis.geography_point_1(pk serial PRIMARY KEY, value geography(POINT));

-- Create a table with 2D POINT geography in NAD83 longlat:
CREATE TABLE struct_it_pg2pg_postgis.geography_point_2(pk serial PRIMARY KEY, value geography(POINT,4269));

-- Create a table with 3D (XYZ) POINTs and an explicit SRID of 4326:
CREATE TABLE struct_it_pg2pg_postgis.geography_pointz_1(pk serial PRIMARY KEY, value geography(POINTZ,4326));

CREATE TABLE struct_it_pg2pg_postgis.geography_multipoint_1(pk serial PRIMARY KEY, value geography(MULTIPOINT, 4326));

------------------------------------- Geography, LINE
-- Create a table with 2D LINESTRING geography with the default SRID 4326:
CREATE TABLE struct_it_pg2pg_postgis.geography_linestring_1(pk serial PRIMARY KEY, value geography(LINESTRING));

-- Create a table with 4D (XYZM) LINESTRING geometry with the default SRID:
CREATE TABLE struct_it_pg2pg_postgis.geography_linestringgzm_1(pk serial PRIMARY KEY, value geography(LINESTRINGZM));

-- Create a table with 3D LineStrings and an explicit SRID of 4326:
CREATE TABLE struct_it_pg2pg_postgis.geography_linestringgz_1(pk serial PRIMARY KEY, value geography(LINESTRINGZ,4326));

CREATE TABLE struct_it_pg2pg_postgis.geography_multilinestring_1(pk serial PRIMARY KEY, value geography(MULTILINESTRING, 4326));

------------------------------------- Geography, POLYGON
CREATE TABLE struct_it_pg2pg_postgis.geography_polygon_1(pk serial PRIMARY KEY, value geography(POLYGON,4267));

CREATE TABLE struct_it_pg2pg_postgis.geography_multipolygon_1(pk serial PRIMARY KEY, value geography(MULTIPOLYGON, 4326));

------------------------------------- Geography, COLLECTION
CREATE TABLE struct_it_pg2pg_postgis.geography_collection_1(pk serial PRIMARY KEY, value geography(GEOMETRYCOLLECTION, 4326));


------------------------------------- Geometry ---------------------------------------
-- Create a table holding any kind of geometry with the default SRID:
CREATE TABLE struct_it_pg2pg_postgis.geometry_1(pk serial PRIMARY KEY, value geometry);

------------------------------------- Geometry, POINT
-- Create a table with 2D POINT geometry with the default SRID:
CREATE TABLE struct_it_pg2pg_postgis.geometry_point_1(pk serial PRIMARY KEY, value geometry(POINT));

-- Create a table with 3D (XYZ) POINTs and an explicit SRID of 3005:
CREATE TABLE struct_it_pg2pg_postgis.geometry_pointz_1(pk serial PRIMARY KEY, value geometry(POINTZ,3005));

CREATE TABLE struct_it_pg2pg_postgis.geometry_multipoint_1(pk serial PRIMARY KEY, value geometry(MULTIPOINT, 4326) );

------------------------------------- Geometry, LINE
-- Create a table with a geometry column storing 2D (XY) LineStrings in the BC-Albers coordinate system (SRID 3005)
CREATE TABLE struct_it_pg2pg_postgis.geometry_linestring_1 (pk SERIAL PRIMARY KEY, value geometry(LINESTRING,3005));

-- Create a table with 4D (XYZM) LINESTRING geometry with the default SRID:
CREATE TABLE struct_it_pg2pg_postgis.geometry_linestringgzm_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZM));

-- Create a table with 3D LineStrings and an explicit SRID of 4326:
CREATE TABLE struct_it_pg2pg_postgis.geometry_linestringgz_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZ,4326));

CREATE TABLE struct_it_pg2pg_postgis.geometry_multilinestring_1(pk serial PRIMARY KEY, value geometry(MULTILINESTRING, 4326));

------------------------------------- Geometry, POLYGON
-- Create a table with 2D POLYGON geometry with the SRID 4267 (NAD 1927 long lat):
CREATE TABLE struct_it_pg2pg_postgis.geometry_polygon_1(pk serial PRIMARY KEY, value geometry(POLYGON,4267));

CREATE TABLE struct_it_pg2pg_postgis.geometry_multipolygon_1(pk serial PRIMARY KEY, value geometry(MULTIPOLYGON, 4326));

------------------------------------- Geometry, COLLECTION
CREATE TABLE struct_it_pg2pg_postgis.geometry_collection_1(pk serial PRIMARY KEY, value geometry(GEOMETRYCOLLECTION, 4326));

------------------------------------- Others ---------------------------------------

CREATE TABLE struct_it_pg2pg_postgis.box_1(pk serial PRIMARY KEY, value BOX);

CREATE TABLE struct_it_pg2pg_postgis.box2d_1(pk serial PRIMARY KEY, value BOX2D);

CREATE TABLE struct_it_pg2pg_postgis.circle_1(pk serial PRIMARY KEY, value CIRCLE);

CREATE TABLE struct_it_pg2pg_postgis.path_1(pk serial PRIMARY KEY, value PATH);

CREATE TABLE struct_it_pg2pg_postgis.point_1(pk serial PRIMARY KEY, value POINT);

CREATE TABLE struct_it_pg2pg_postgis.polygon_1(pk serial PRIMARY KEY, value POLYGON);
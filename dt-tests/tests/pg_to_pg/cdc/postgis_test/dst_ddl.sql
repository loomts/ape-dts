CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS isn;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgis;

-- refer: https://postgis.net/docs/using_postgis_dbmanagement.html

------------------------------------- Geography ---------------------------------------
DROP TABLE IF EXISTS geography_1;
CREATE TABLE geography_1(pk serial PRIMARY KEY, value geography);

------------------------------------- Geography, POINT
-- Create a table with 2D POINT geography with the default SRID 4326 (WGS84 long/lat):
DROP TABLE IF EXISTS geography_point_1;
CREATE TABLE geography_point_1(pk serial PRIMARY KEY, value geography(POINT));

-- Create a table with 2D POINT geography in NAD83 longlat:
DROP TABLE IF EXISTS geography_point_2;
CREATE TABLE geography_point_2(pk serial PRIMARY KEY, value geography(POINT,4269));

-- Create a table with 3D (XYZ) POINTs and an explicit SRID of 4326:
DROP TABLE IF EXISTS geography_pointz_1;
CREATE TABLE geography_pointz_1(pk serial PRIMARY KEY, value geography(POINTZ,4326));

DROP TABLE IF EXISTS geography_multipoint_1;
CREATE TABLE geography_multipoint_1(pk serial PRIMARY KEY, value geography(MULTIPOINT, 4326));

------------------------------------- Geography, LINE
-- Create a table with 2D LINESTRING geography with the default SRID 4326:
DROP TABLE IF EXISTS geography_linestring_1;
CREATE TABLE geography_linestring_1(pk serial PRIMARY KEY, value geography(LINESTRING));

-- Create a table with 4D (XYZM) LINESTRING geometry with the default SRID:
DROP TABLE IF EXISTS geography_linestringgzm_1;
CREATE TABLE geography_linestringgzm_1(pk serial PRIMARY KEY, value geography(LINESTRINGZM));

-- Create a table with 3D LineStrings and an explicit SRID of 4326:
DROP TABLE IF EXISTS geography_linestringgz_1;
CREATE TABLE geography_linestringgz_1(pk serial PRIMARY KEY, value geography(LINESTRINGZ,4326));

DROP TABLE IF EXISTS geography_multilinestring_1;
CREATE TABLE geography_multilinestring_1(pk serial PRIMARY KEY, value geography(MULTILINESTRING, 4326));

------------------------------------- Geography, POLYGON
DROP TABLE IF EXISTS geography_polygon_1;
CREATE TABLE geography_polygon_1(pk serial PRIMARY KEY, value geography(POLYGON,4267));

DROP TABLE IF EXISTS geography_multipolygon_1;
CREATE TABLE geography_multipolygon_1(pk serial PRIMARY KEY, value geography(MULTIPOLYGON, 4326));

------------------------------------- Geography, COLLECTION
DROP TABLE IF EXISTS geography_collection_1;
CREATE TABLE geography_collection_1(pk serial PRIMARY KEY, value geography(GEOMETRYCOLLECTION, 4326));


------------------------------------- Geometry ---------------------------------------
-- Create a table holding any kind of geometry with the default SRID:
DROP TABLE IF EXISTS geometry_1;
CREATE TABLE geometry_1(pk serial PRIMARY KEY, value geometry);

------------------------------------- Geometry, POINT
-- Create a table with 2D POINT geometry with the default SRID:
DROP TABLE IF EXISTS geometry_point_1;
CREATE TABLE geometry_point_1(pk serial PRIMARY KEY, value geometry(POINT));

-- Create a table with 3D (XYZ) POINTs and an explicit SRID of 3005:
DROP TABLE IF EXISTS geometry_pointz_1;
CREATE TABLE geometry_pointz_1(pk serial PRIMARY KEY, value geometry(POINTZ,3005));

DROP TABLE IF EXISTS geometry_multipoint_1;
CREATE TABLE geometry_multipoint_1(pk serial PRIMARY KEY, value geometry(MULTIPOINT, 4326) );

------------------------------------- Geometry, LINE
-- Create a table with a geometry column storing 2D (XY) LineStrings in the BC-Albers coordinate system (SRID 3005)
DROP TABLE IF EXISTS geometry_linestring_1;
CREATE TABLE geometry_linestring_1 (pk SERIAL PRIMARY KEY, value geometry(LINESTRING,3005));

-- Create a table with 4D (XYZM) LINESTRING geometry with the default SRID:
DROP TABLE IF EXISTS geometry_linestringgzm_1;
CREATE TABLE geometry_linestringgzm_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZM));

-- Create a table with 3D LineStrings and an explicit SRID of 4326:
DROP TABLE IF EXISTS geometry_linestringgz_1;
CREATE TABLE geometry_linestringgz_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZ,4326));

DROP TABLE IF EXISTS geometry_multilinestring_1;
CREATE TABLE geometry_multilinestring_1(pk serial PRIMARY KEY, value geometry(MULTILINESTRING, 4326));

------------------------------------- Geometry, POLYGON
-- Create a table with 2D POLYGON geometry with the SRID 4267 (NAD 1927 long lat):
DROP TABLE IF EXISTS geometry_polygon_1;
CREATE TABLE geometry_polygon_1(pk serial PRIMARY KEY, value geometry(POLYGON,4267));

DROP TABLE IF EXISTS geometry_multipolygon_1;
CREATE TABLE geometry_multipolygon_1(pk serial PRIMARY KEY, value geometry(MULTIPOLYGON, 4326));

------------------------------------- Geometry, COLLECTION
DROP TABLE IF EXISTS geometry_collection_1;
CREATE TABLE geometry_collection_1(pk serial PRIMARY KEY, value geometry(GEOMETRYCOLLECTION, 4326));

------------------------------------- Others ---------------------------------------

DROP TABLE IF EXISTS box_1;
CREATE TABLE box_1(pk serial PRIMARY KEY, value BOX);

DROP TABLE IF EXISTS box2d_1;
CREATE TABLE box2d_1(pk serial PRIMARY KEY, value BOX2D);

DROP TABLE IF EXISTS circle_1;
CREATE TABLE circle_1(pk serial PRIMARY KEY, value CIRCLE);

DROP TABLE IF EXISTS path_1;
CREATE TABLE path_1(pk serial PRIMARY KEY, value PATH);

DROP TABLE IF EXISTS point_1;
CREATE TABLE point_1(pk serial PRIMARY KEY, value POINT);

DROP TABLE IF EXISTS polygon_1;
CREATE TABLE polygon_1(pk serial PRIMARY KEY, value POLYGON);

------------------------------------- Geography ---------------------------------------
-- CREATE TABLE geography_1(pk serial PRIMARY KEY, value geography);
INSERT INTO geography_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geography,'POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_1 VALUES (2, ARRAY['MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography,'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography]);
INSERT INTO geography_1 VALUES (3, ARRAY['SRID=4326;POINT(174.9479 -36.7208)'::geography,'SRID=4326;POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_1 VALUES (4, ARRAY['SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography,'SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography]);
INSERT INTO geography_1 VALUES (5, NULL);
UPDATE geography_1 set value = (select value from geography_1 where pk = 1) WHERE pk = 3;
UPDATE geography_1 set value = (select value from geography_1 where pk = 2) WHERE pk = 1;
UPDATE geography_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_1;

------------------------------------- Geography, POINT
-- CREATE TABLE geography_point_1(pk serial PRIMARY KEY, value geography(POINT));
INSERT INTO geography_point_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geography,'POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_point_1 VALUES (2, ARRAY['SRID=4326;POINT(169.1321 -44.7032)'::geography,'SRID=4326;POINT(169.1321 -44.7032)'::geography]);
INSERT INTO geography_point_1 VALUES (3, NULL);
UPDATE geography_point_1 set value = (select value from geography_point_1 where pk = 1) WHERE pk = 3;
UPDATE geography_point_1 set value = (select value from geography_point_1 where pk = 2) WHERE pk = 1;
UPDATE geography_point_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_point_1;

-- CREATE TABLE geography_point_2(pk serial PRIMARY KEY, value geography(POINT,4269));
INSERT INTO geography_point_2 VALUES (1, ARRAY['SRID=4269;POINT(174.9479 -36.7208)'::geography,'SRID=4269;POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_point_2 VALUES (2, ARRAY['SRID=4269;POINT(169.1321 -44.7032)'::geography,'SRID=4269;POINT(169.1321 -44.7032)'::geography]);
INSERT INTO geography_point_2 VALUES (3, NULL);
UPDATE geography_point_2 set value = (select value from geography_point_2 where pk = 1) WHERE pk = 3;
UPDATE geography_point_2 set value = (select value from geography_point_2 where pk = 2) WHERE pk = 1;
UPDATE geography_point_2 set value = NULL WHERE pk = 2;
DELETE FROM geography_point_2;

-- CREATE TABLE geography_pointz_1(pk serial PRIMARY KEY, value geography(POINTZ,4326));
INSERT INTO geography_pointz_1 VALUES (1, ARRAY['POINTZ(40.7128 -74.0060 10)'::geography,'POINTZ(40.7128 -74.0060 10)'::geography]);
INSERT INTO geography_pointz_1 VALUES (2, ARRAY['SRID=4326;POINTZ(40 -74 10)'::geography,'SRID=4326;POINTZ(40 -74 10)'::geography]);
INSERT INTO geography_pointz_1 VALUES (3, NULL);
UPDATE geography_pointz_1 set value = (select value from geography_pointz_1 where pk = 1) WHERE pk = 3;
UPDATE geography_pointz_1 set value = (select value from geography_pointz_1 where pk = 2) WHERE pk = 1;
UPDATE geography_pointz_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_pointz_1;

-- CREATE TABLE geography_multipoint_1(pk serial PRIMARY KEY, value geography(MULTIPOINT, 4326));
INSERT INTO geography_multipoint_1 VALUES (1, ARRAY['MULTIPOINT(40.7128 -74.0060)'::geography,'MULTIPOINT(40.7128 -74.0060)'::geography]);
INSERT INTO geography_multipoint_1 VALUES (2, ARRAY['SRID=4326;MULTIPOINT(40 -74)'::geography,'SRID=4326;MULTIPOINT(40 -74)'::geography]);
INSERT INTO geography_multipoint_1 VALUES (3, NULL);
UPDATE geography_multipoint_1 set value = (select value from geography_multipoint_1 where pk = 1) WHERE pk = 3;
UPDATE geography_multipoint_1 set value = (select value from geography_multipoint_1 where pk = 2) WHERE pk = 1;
UPDATE geography_multipoint_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_multipoint_1;

------------------------------------- Geography, LINE
-- CREATE TABLE geography_linestring_1(pk serial PRIMARY KEY, value geography(LINESTRING));
INSERT INTO geography_linestring_1 VALUES (1, ARRAY['LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography,'LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography]);
INSERT INTO geography_linestring_1 VALUES (2, ARRAY['SRID=4326;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography,'SRID=4326;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography]);
INSERT INTO geography_linestring_1 VALUES (3, NULL);
UPDATE geography_linestring_1 set value = (select value from geography_linestring_1 where pk = 1) WHERE pk = 3;
UPDATE geography_linestring_1 set value = (select value from geography_linestring_1 where pk = 2) WHERE pk = 1;
UPDATE geography_linestring_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_linestring_1;

-- CREATE TABLE geography_linestringgzm_1(pk serial PRIMARY KEY, value geography(LINESTRINGZM));
INSERT INTO geography_linestringgzm_1 VALUES (1, ARRAY['LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography,'LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography]);
INSERT INTO geography_linestringgzm_1 VALUES (2, ARRAY['SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography,'SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography]);
INSERT INTO geography_linestringgzm_1 VALUES (3, NULL);
UPDATE geography_linestringgzm_1 set value = (select value from geography_linestringgzm_1 where pk = 1) WHERE pk = 3;
UPDATE geography_linestringgzm_1 set value = (select value from geography_linestringgzm_1 where pk = 2) WHERE pk = 1;
UPDATE geography_linestringgzm_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_linestringgzm_1;

-- CREATE TABLE geography_linestringgz_1(pk serial PRIMARY KEY, value geography(LINESTRINGZ,4326));
INSERT INTO geography_linestringgz_1 VALUES (1, ARRAY[ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)')]);
INSERT INTO geography_linestringgz_1 VALUES (2, ARRAY[ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)')]);
INSERT INTO geography_linestringgz_1 VALUES (3, NULL);
UPDATE geography_linestringgz_1 set value = (select value from geography_linestringgz_1 where pk = 1) WHERE pk = 3;
UPDATE geography_linestringgz_1 set value = (select value from geography_linestringgz_1 where pk = 2) WHERE pk = 1;
UPDATE geography_linestringgz_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_linestringgz_1;

-- CREATE TABLE geography_multilinestring_1(pk serial PRIMARY KEY, value geography(MULTILINESTRING, 4326));
INSERT INTO geography_multilinestring_1 VALUES (1, ARRAY['MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography,'MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography]);
INSERT INTO geography_multilinestring_1 VALUES (2, ARRAY['SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography,'SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography]);
INSERT INTO geography_multilinestring_1 VALUES (3, NULL);
UPDATE geography_multilinestring_1 set value = (select value from geography_multilinestring_1 where pk = 1) WHERE pk = 3;
UPDATE geography_multilinestring_1 set value = (select value from geography_multilinestring_1 where pk = 2) WHERE pk = 1;
UPDATE geography_multilinestring_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_multilinestring_1;

------------------------------------- Geography, POLYGON
-- CREATE TABLE geography_polygon_1(pk serial PRIMARY KEY, value geography(POLYGON,4267));
INSERT INTO geography_polygon_1 VALUES (1, ARRAY[ST_GeographyFromText('SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'),ST_GeographyFromText('SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))')]);
INSERT INTO geography_polygon_1 VALUES (2, NULL);
UPDATE geography_polygon_1 set value = (select value from geography_polygon_1 where pk = 2) WHERE pk = 1;
UPDATE geography_polygon_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_polygon_1;

-- CREATE TABLE geography_multipolygon_1(pk serial PRIMARY KEY, value geography(MULTIPOLYGON, 4326));
INSERT INTO geography_multipolygon_1 VALUES (1, ARRAY['MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography,'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography]);
INSERT INTO geography_multipolygon_1 VALUES (2, ARRAY['SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography,'SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography]);
INSERT INTO geography_multipolygon_1 VALUES (3, NULL);
UPDATE geography_multipolygon_1 set value = (select value from geography_multipolygon_1 where pk = 1) WHERE pk = 3;
UPDATE geography_multipolygon_1 set value = (select value from geography_multipolygon_1 where pk = 2) WHERE pk = 1;
UPDATE geography_multipolygon_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_multipolygon_1;

------------------------------------- Geography, COLLECTION
-- CREATE TABLE geography_collection_1(pk serial PRIMARY KEY, value geography(GEOMETRYCOLLECTION, 4326));
INSERT INTO geography_collection_1 VALUES (1, ARRAY['GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography,'GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography]);
INSERT INTO geography_collection_1 VALUES (2, ARRAY['SRID=4326;GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography,'SRID=4326;GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography]);
INSERT INTO geography_collection_1 VALUES (3, NULL);
UPDATE geography_collection_1 set value = (select value from geography_collection_1 where pk = 1) WHERE pk = 3;
UPDATE geography_collection_1 set value = (select value from geography_collection_1 where pk = 2) WHERE pk = 1;
UPDATE geography_collection_1 set value = NULL WHERE pk = 2;
DELETE FROM geography_collection_1;

------------------------------------- Geometry ---------------------------------------
-- CREATE TABLE geometry_1(pk serial PRIMARY KEY, value geometry);
INSERT INTO geometry_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geometry,'POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_1 VALUES (2, ARRAY['MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry,'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry]);
INSERT INTO geometry_1 VALUES (3, ARRAY['SRID=4326;POINT(174.9479 -36.7208)'::geometry,'SRID=4326;POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_1 VALUES (4, ARRAY['SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry,'SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry]);
INSERT INTO geometry_1 VALUES (5, NULL);
UPDATE geometry_1 set value = (select value from geometry_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_1 set value = (select value from geometry_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_1;

------------------------------------- Geometry, POINT
-- CREATE TABLE geometry_point_1(pk serial PRIMARY KEY, value geometry(POINT));
INSERT INTO geometry_point_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geometry,'POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_point_1 VALUES (2, ARRAY['SRID=4326;POINT(169.1321 -44.7032)'::geometry,'SRID=4326;POINT(169.1321 -44.7032)'::geometry]);
INSERT INTO geometry_point_1 VALUES (3, NULL);
UPDATE geometry_point_1 set value = (select value from geometry_point_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_point_1 set value = (select value from geometry_point_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_point_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_point_1;

-- CREATE TABLE geometry_pointz_1(pk serial PRIMARY KEY, value geometry(POINTZ,3005));       
INSERT INTO geometry_pointz_1 VALUES (1, ARRAY['POINTZ(40.7128 -74.0060 10)'::geometry,'POINTZ(40.7128 -74.0060 10)'::geometry]);
INSERT INTO geometry_pointz_1 VALUES (2, ARRAY['SRID=3005;POINTZ(40 -74 10)'::geometry,'SRID=3005;POINTZ(40 -74 10)'::geometry]);
INSERT INTO geometry_pointz_1 VALUES (3, NULL);
UPDATE geometry_pointz_1 set value = (select value from geometry_pointz_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_pointz_1 set value = (select value from geometry_pointz_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_pointz_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_pointz_1;

-- CREATE TABLE geometry_multipoint_1(pk serial PRIMARY KEY, value geometry(MULTIPOINT, 4326) );
INSERT INTO geometry_multipoint_1 VALUES (1, ARRAY['SRID=4326;MULTIPOINT(40.7128 -74.0060)'::geometry,'SRID=4326;MULTIPOINT(40.7128 -74.0060)'::geometry]);
INSERT INTO geometry_multipoint_1 VALUES (2, ARRAY['SRID=4326;MULTIPOINT(40 -74)'::geometry,'SRID=4326;MULTIPOINT(40 -74)'::geometry]);
INSERT INTO geometry_multipoint_1 VALUES (3, NULL);
UPDATE geometry_multipoint_1 set value = (select value from geometry_multipoint_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_multipoint_1 set value = (select value from geometry_multipoint_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_multipoint_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_multipoint_1;

------------------------------------- Geometry, LINE
-- CREATE TABLE geometry_linestring_1 (id SERIAL PRIMARY KEY, value geometry(LINESTRING,3005));
INSERT INTO geometry_linestring_1 VALUES (1, ARRAY['LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry,'LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry]);
INSERT INTO geometry_linestring_1 VALUES (2, ARRAY['SRID=3005;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry,'SRID=3005;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry]);
INSERT INTO geometry_linestring_1 VALUES (3, NULL);
UPDATE geometry_linestring_1 set value = (select value from geometry_linestring_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_linestring_1 set value = (select value from geometry_linestring_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_linestring_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_linestring_1;

-- CREATE TABLE geometry_linestringgzm_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZM));
INSERT INTO geometry_linestringgzm_1 VALUES (1, ARRAY['LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry,'LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry]);
INSERT INTO geometry_linestringgzm_1 VALUES (2, ARRAY['SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry,'SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry]);
INSERT INTO geometry_linestringgzm_1 VALUES (3, NULL);
UPDATE geometry_linestringgzm_1 set value = (select value from geometry_linestringgzm_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_linestringgzm_1 set value = (select value from geometry_linestringgzm_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_linestringgzm_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_linestringgzm_1;

-- CREATE TABLE geometry_linestringgz_1(pk serial PRIMARY KEY, value geometry(LINESTRINGZ,4326));
INSERT INTO geometry_linestringgz_1 VALUES (1, ARRAY[ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)')]);
INSERT INTO geometry_linestringgz_1 VALUES (2, ARRAY[ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)')]);
INSERT INTO geometry_linestringgz_1 VALUES (3, NULL);
UPDATE geometry_linestringgz_1 set value = (select value from geometry_linestringgz_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_linestringgz_1 set value = (select value from geometry_linestringgz_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_linestringgz_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_linestringgz_1;

-- CREATE TABLE geometry_multilinestring_1(pk serial PRIMARY KEY, value geometry(MULTILINESTRING, 4326));
INSERT INTO geometry_multilinestring_1 VALUES (1, ARRAY['MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry,'MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry]);
INSERT INTO geometry_multilinestring_1 VALUES (2, ARRAY['SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry,'SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry]);
INSERT INTO geometry_multilinestring_1 VALUES (3, NULL);
UPDATE geometry_multilinestring_1 set value = (select value from geometry_multilinestring_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_multilinestring_1 set value = (select value from geometry_multilinestring_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_multilinestring_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_multilinestring_1;

------------------------------------- Geometry, POLYGON
-- CREATE TABLE geometry_polygon_1(pk serial PRIMARY KEY, value geometry(POLYGON,4267));
INSERT INTO geometry_polygon_1 VALUES (1, ARRAY['POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry,'POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry]);
INSERT INTO geometry_polygon_1 VALUES (2, ARRAY['SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry,'SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry]);
INSERT INTO geometry_polygon_1 VALUES (3, NULL);
UPDATE geometry_polygon_1 set value = (select value from geometry_polygon_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_polygon_1 set value = (select value from geometry_polygon_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_polygon_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_polygon_1;

-- CREATE TABLE geometry_multipolygon_1(pk serial PRIMARY KEY, value geometry(MULTIPOLYGON, 4326));
INSERT INTO geometry_multipolygon_1 VALUES (1, ARRAY['MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry,'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry]);
INSERT INTO geometry_multipolygon_1 VALUES (2, ARRAY['SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry,'SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry]);
INSERT INTO geometry_multipolygon_1 VALUES (3, NULL);
UPDATE geometry_multipolygon_1 set value = (select value from geometry_multipolygon_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_multipolygon_1 set value = (select value from geometry_multipolygon_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_multipolygon_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_multipolygon_1;

------------------------------------- Geometry, COLLECTION
-- CREATE TABLE geometry_collection_1(pk serial PRIMARY KEY, value geometry(GEOMETRYCOLLECTION, 4326));
INSERT INTO geometry_collection_1 VALUES (1, ARRAY['GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(2 2,3 3))','GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(2 2,3 3))']);
INSERT INTO geometry_collection_1 VALUES (2, ARRAY['GEOMETRYCOLLECTION(POINT(2 2),LINESTRING(2 2,3 3))','GEOMETRYCOLLECTION(POINT(2 2),LINESTRING(2 2,3 3))']);
INSERT INTO geometry_collection_1 VALUES (3, NULL);
UPDATE geometry_collection_1 set value = (select value from geometry_collection_1 where pk = 1) WHERE pk = 3;
UPDATE geometry_collection_1 set value = (select value from geometry_collection_1 where pk = 2) WHERE pk = 1;
UPDATE geometry_collection_1 set value = NULL WHERE pk = 2;
DELETE FROM geometry_collection_1;

------------------------------------- Others ---------------------------------------

-- CREATE TABLE box_1(pk serial PRIMARY KEY, value BOX);
INSERT INTO box_1 VALUES (1, ARRAY['(1.0,1.0),(0.0,0.0)'::box,'(1.0,1.0),(0.0,0.0)'::box]);
INSERT INTO box_1 VALUES (2, NULL);
UPDATE box_1 set value = (select value from box_1 where pk = 1) WHERE pk = 2;
UPDATE box_1 set value = NULL WHERE pk = 1;
DELETE FROM box_1;

-- CREATE TABLE box2d_1(pk serial PRIMARY KEY, value BOX2D);
INSERT INTO box2d_1 VALUES (1, ARRAY[Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')),Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))]);
INSERT INTO box2d_1 VALUES (2, NULL);
UPDATE box2d_1 set value = (select value from box2d_1 where pk = 1) WHERE pk = 2;
UPDATE box2d_1 set value = NULL WHERE pk = 1;
DELETE FROM box2d_1;

-- CREATE TABLE circle_1(pk serial PRIMARY KEY, value CIRCLE);
INSERT INTO circle_1 VALUES (1, ARRAY[circle '((0,0),5)',circle '((0,0),5)']);
INSERT INTO circle_1 VALUES (2, NULL);
UPDATE circle_1 set value = (select value from circle_1 where pk = 1) WHERE pk = 2;
UPDATE circle_1 set value = NULL WHERE pk = 1;
DELETE FROM circle_1;

-- CREATE TABLE path_1(pk serial PRIMARY KEY, value PATH);
INSERT INTO path_1 VALUES (1, ARRAY['[(0,0),(0,1),(1,1),(1,0),(0,0)]'::path, '[(0,0),(0,1),(1,1),(1,0),(0,0)]'::path]);
INSERT INTO path_1 VALUES (2, NULL);
UPDATE path_1 set value = (select value from path_1 where pk = 1) WHERE pk = 2;
UPDATE path_1 set value = NULL WHERE pk = 1;
DELETE FROM path_1;

-- CREATE TABLE point_1(pk serial PRIMARY KEY, value POINT);
INSERT INTO point_1 VALUES (1, ARRAY['(1.0,1.0)'::POINT,'(1.0,1.0)'::POINT]);
INSERT INTO point_1 VALUES (2, NULL);
UPDATE point_1 set value = (select value from point_1 where pk = 1) WHERE pk = 2;
UPDATE point_1 set value = NULL WHERE pk = 1;
DELETE FROM point_1;

-- CREATE TABLE polygon_1(pk serial PRIMARY KEY, value POLYGON);
INSERT INTO polygon_1 VALUES (1, ARRAY['((0.0,0.0),(0.0,1.0),(1.0,1.0))'::POLYGON,'((0.0,0.0),(0.0,1.0),(1.0,1.0))'::POLYGON]);
INSERT INTO polygon_1 VALUES (2, NULL);
UPDATE polygon_1 set value = (select value from polygon_1 where pk = 1) WHERE pk = 2;
UPDATE polygon_1 set value = NULL WHERE pk = 1;
DELETE FROM polygon_1;
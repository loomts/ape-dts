
------------------------------------- Geography ---------------------------------------
-- CREATE TABLE geography_1(gid serial PRIMARY KEY, geog geography);
INSERT INTO geography_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geography,'POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_1 VALUES (2, ARRAY['MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography,'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography]);
INSERT INTO geography_1 VALUES (3, ARRAY['SRID=4326;POINT(174.9479 -36.7208)'::geography,'SRID=4326;POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_1 VALUES (4, ARRAY['SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography,'SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography]);
INSERT INTO geography_1 VALUES (5, ARRAY[NULL::geography,NULL::geography]);

------------------------------------- Geography, POINT
-- CREATE TABLE geography_point_1(gid serial PRIMARY KEY, geog geography(POINT));
INSERT INTO geography_point_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geography,'POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_point_1 VALUES (2, ARRAY['SRID=4326;POINT(169.1321 -44.7032)'::geography,'SRID=4326;POINT(169.1321 -44.7032)'::geography]);
INSERT INTO geography_point_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_point_2(gid serial PRIMARY KEY, geog geography(POINT,4269));
INSERT INTO geography_point_2 VALUES (1, ARRAY['SRID=4269;POINT(174.9479 -36.7208)'::geography,'SRID=4269;POINT(174.9479 -36.7208)'::geography]);
INSERT INTO geography_point_2 VALUES (2, ARRAY['SRID=4269;POINT(169.1321 -44.7032)'::geography,'SRID=4269;POINT(169.1321 -44.7032)'::geography]);
INSERT INTO geography_point_2 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_pointz_1(gid serial PRIMARY KEY, geog geography(POINTZ,4326));
INSERT INTO geography_pointz_1 VALUES (1, ARRAY['POINTZ(40.7128 -74.0060 10)'::geography,'POINTZ(40.7128 -74.0060 10)'::geography]);
INSERT INTO geography_pointz_1 VALUES (2, ARRAY['SRID=4326;POINTZ(40 -74 10)'::geography,'SRID=4326;POINTZ(40 -74 10)'::geography]);
INSERT INTO geography_pointz_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_multipoint_1(gid serial PRIMARY KEY, geom geography(MULTIPOINT, 4326));
INSERT INTO geography_multipoint_1 VALUES (1, ARRAY['MULTIPOINT(40.7128 -74.0060)'::geography,'MULTIPOINT(40.7128 -74.0060)'::geography]);
INSERT INTO geography_multipoint_1 VALUES (2, ARRAY['SRID=4326;MULTIPOINT(40 -74)'::geography,'SRID=4326;MULTIPOINT(40 -74)'::geography]);
INSERT INTO geography_multipoint_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

------------------------------------- Geography, LINE
-- CREATE TABLE geography_linestring_1(gid serial PRIMARY KEY, geog geography(LINESTRING));
INSERT INTO geography_linestring_1 VALUES (1, ARRAY['LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography,'LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography]);
INSERT INTO geography_linestring_1 VALUES (2, ARRAY['SRID=4326;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography,'SRID=4326;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geography]);
INSERT INTO geography_linestring_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_linestringgzm_1(gid serial PRIMARY KEY, geom geography(LINESTRINGZM));
INSERT INTO geography_linestringgzm_1 VALUES (1, ARRAY['LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography,'LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography]);
INSERT INTO geography_linestringgzm_1 VALUES (2, ARRAY['SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography,'SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geography]);
INSERT INTO geography_linestringgzm_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_linestringgz_1(gid serial PRIMARY KEY, geom geography(LINESTRINGZ,4326));
INSERT INTO geography_linestringgz_1 VALUES (1, ARRAY[ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)')]);
INSERT INTO geography_linestringgz_1 VALUES (2, ARRAY[ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)')]);
INSERT INTO geography_linestringgz_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_multilinestring_1(gid serial PRIMARY KEY, geom geography(MULTILINESTRING, 4326));
INSERT INTO geography_multilinestring_1 VALUES (1, ARRAY['MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography,'MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography]);
INSERT INTO geography_multilinestring_1 VALUES (2, ARRAY['SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography,'SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geography]);
INSERT INTO geography_multilinestring_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

------------------------------------- Geography, POLYGON
-- CREATE TABLE geography_polygon_1(gid serial PRIMARY KEY, geom geography(POLYGON,4267));
INSERT INTO geography_polygon_1 VALUES (1, ARRAY[ST_GeographyFromText('SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'),ST_GeographyFromText('SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))')]);
INSERT INTO geography_polygon_1 VALUES (2, ARRAY[NULL::geography,NULL::geography]);

-- CREATE TABLE geography_multipolygon_1(gid serial PRIMARY KEY, geom geography(MULTIPOLYGON, 4326));
INSERT INTO geography_multipolygon_1 VALUES (1, ARRAY['MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography,'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography]);
INSERT INTO geography_multipolygon_1 VALUES (2, ARRAY['SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography,'SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geography]);
INSERT INTO geography_multipolygon_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

------------------------------------- Geography, COLLECTION
-- CREATE TABLE geography_collection_1(gid serial PRIMARY KEY, geom geography(GEOMETRYCOLLECTION, 4326));
INSERT INTO geography_collection_1 VALUES (1, ARRAY['GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography,'GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography]);
INSERT INTO geography_collection_1 VALUES (2, ARRAY['SRID=4326;GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography,'SRID=4326;GEOMETRYCOLLECTION(POINT(-122.431297 37.773972), LINESTRING(-122.431297 37.773972, -122.430738 37.773738))'::geography]);
INSERT INTO geography_collection_1 VALUES (3, ARRAY[NULL::geography,NULL::geography]);

------------------------------------- Geometry ---------------------------------------
-- CREATE TABLE geometry_1(gid serial PRIMARY KEY, geom geometry);
INSERT INTO geometry_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geometry,'POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_1 VALUES (2, ARRAY['MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry,'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry]);
INSERT INTO geometry_1 VALUES (3, ARRAY['SRID=4326;POINT(174.9479 -36.7208)'::geometry,'SRID=4326;POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_1 VALUES (4, ARRAY['SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry,'SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geometry]);
INSERT INTO geometry_1 VALUES (5, ARRAY[NULL,NULL]);

------------------------------------- Geometry, POINT
-- CREATE TABLE geometry_point_1(gid serial PRIMARY KEY, geom geometry(POINT));
INSERT INTO geometry_point_1 VALUES (1, ARRAY['POINT(174.9479 -36.7208)'::geometry,'POINT(174.9479 -36.7208)'::geometry]);
INSERT INTO geometry_point_1 VALUES (2, ARRAY['SRID=4326;POINT(169.1321 -44.7032)'::geometry,'SRID=4326;POINT(169.1321 -44.7032)'::geometry]);
INSERT INTO geometry_point_1 VALUES (3, ARRAY[NULL,NULL]);

-- CREATE TABLE geometry_pointz_1(gid serial PRIMARY KEY, geom geometry(POINTZ,3005));       
INSERT INTO geometry_pointz_1 VALUES (1, ARRAY['POINTZ(40.7128 -74.0060 10)'::geometry,'POINTZ(40.7128 -74.0060 10)'::geometry]);
INSERT INTO geometry_pointz_1 VALUES (2, ARRAY['SRID=3005;POINTZ(40 -74 10)'::geometry,'SRID=3005;POINTZ(40 -74 10)'::geometry]);
INSERT INTO geometry_pointz_1 VALUES (3, ARRAY[NULL,NULL]);
            
-- CREATE TABLE geometry_multipoint_1(gid serial PRIMARY KEY, geom geometry(MULTIPOINT, 4326) );
INSERT INTO geometry_multipoint_1 VALUES (1, ARRAY['SRID=4326;MULTIPOINT(40.7128 -74.0060)'::geometry,'SRID=4326;MULTIPOINT(40.7128 -74.0060)'::geometry]);
INSERT INTO geometry_multipoint_1 VALUES (2, ARRAY['SRID=4326;MULTIPOINT(40 -74)'::geometry,'SRID=4326;MULTIPOINT(40 -74)'::geometry]);
INSERT INTO geometry_multipoint_1 VALUES (3, ARRAY[NULL,NULL]);

------------------------------------- Geometry, LINE
-- CREATE TABLE geometry_linestring_1 (id SERIAL PRIMARY KEY, geom geometry(LINESTRING,3005));
INSERT INTO geometry_linestring_1 VALUES (1, ARRAY['LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry,'LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry]);
INSERT INTO geometry_linestring_1 VALUES (2, ARRAY['SRID=3005;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry,'SRID=3005;LINESTRING(-122.42 37.78, -122.45 37.91, -122.48 37.73)'::geometry]);
INSERT INTO geometry_linestring_1 VALUES (3, ARRAY[NULL,NULL]);

-- CREATE TABLE geometry_linestringgzm_1(gid serial PRIMARY KEY, geom geometry(LINESTRINGZM));
INSERT INTO geometry_linestringgzm_1 VALUES (1, ARRAY['LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry,'LINESTRINGZM(1 2 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry]);
INSERT INTO geometry_linestringgzm_1 VALUES (2, ARRAY['SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry,'SRID=4326;LINESTRINGZM(1.2345 2.3456 100.0 0.1, 3.4567 4.5678 200.0 0.2, 5.6789 6.7890 300.0 0.3)'::geometry]);
INSERT INTO geometry_linestringgzm_1 VALUES (3, ARRAY[NULL,NULL]);

-- CREATE TABLE geometry_linestringgz_1(gid serial PRIMARY KEY, geom geometry(LINESTRINGZ,4326));
INSERT INTO geometry_linestringgz_1 VALUES (1, ARRAY[ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('LINESTRINGZ(1 1 1, 20 20 2, 30 40 3)')]);
INSERT INTO geometry_linestringgz_1 VALUES (2, ARRAY[ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)'),ST_GeomFromEWKT('SRID=4326;LINESTRINGZ(10 10 1, 20 20 2, 30 40 3)')]);
INSERT INTO geometry_linestringgz_1 VALUES (3, ARRAY[NULL,NULL]);

-- CREATE TABLE geometry_multilinestring_1(gid serial PRIMARY KEY, geom geometry(MULTILINESTRING, 4326));
INSERT INTO geometry_multilinestring_1 VALUES (1, ARRAY['MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry,'MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry]);
INSERT INTO geometry_multilinestring_1 VALUES (2, ARRAY['SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry,'SRID=4326;MULTILINESTRING((-122.358 47.653, -122.348 47.649),(-122.348 47.649, -122.348 47.658))'::geometry]);
INSERT INTO geometry_multilinestring_1 VALUES (3, ARRAY[NULL,NULL]);

------------------------------------- Geometry, POLYGON
-- CREATE TABLE geometry_polygon_1(gid serial PRIMARY KEY, geom geometry(POLYGON,4267));
INSERT INTO geometry_polygon_1 VALUES (1, ARRAY['POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry,'POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry]);
INSERT INTO geometry_polygon_1 VALUES (2, ARRAY['SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry,'SRID=4267;POLYGON((-117.234375 32.84267472990693,-116.71875 32.99023555965106,-116.3671875 32.75405103620088,-116.94736411511119 32.55101160188101,-117.234375 32.84267472990693))'::geometry]);
INSERT INTO geometry_polygon_1 VALUES (3, ARRAY[NULL,NULL]);

-- CREATE TABLE geometry_multipolygon_1(gid serial PRIMARY KEY, geom geometry(MULTIPOLYGON, 4326));
INSERT INTO geometry_multipolygon_1 VALUES (1, ARRAY['MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry,'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry]);
INSERT INTO geometry_multipolygon_1 VALUES (2, ARRAY['SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry,'SRID=4326;MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'::geometry]);
INSERT INTO geometry_multipolygon_1 VALUES (3, ARRAY[NULL,NULL]);

------------------------------------- Geometry, COLLECTION
-- CREATE TABLE geometry_collection_1(gid serial PRIMARY KEY, geom geometry(GEOMETRYCOLLECTION, 4326));
INSERT INTO geometry_collection_1 VALUES (1, ARRAY['GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(2 2,3 3))','GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(2 2,3 3))']);
INSERT INTO geometry_collection_1 VALUES (2, ARRAY['GEOMETRYCOLLECTION(POINT(2 2),LINESTRING(2 2,3 3))','GEOMETRYCOLLECTION(POINT(2 2),LINESTRING(2 2,3 3))']);
INSERT INTO geometry_collection_1 VALUES (3, ARRAY[NULL,NULL]);

------------------------------------- Others ---------------------------------------

-- CREATE TABLE box_1(gid serial PRIMARY KEY, geom BOX);
INSERT INTO box_1 VALUES (1, ARRAY['(1.0,1.0),(0.0,0.0)'::box,'(1.0,1.0),(0.0,0.0)'::box]);
INSERT INTO box_1 VALUES (2, ARRAY[NULL::box,NULL::box]);

-- CREATE TABLE box2d_1(gid serial PRIMARY KEY, geom BOX2D);
INSERT INTO box2d_1 VALUES (1, ARRAY[Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')),Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))]);
INSERT INTO box2d_1 VALUES (2, ARRAY[NULL::box2d,NULL::box2d]);

-- CREATE TABLE circle_1(gid serial PRIMARY KEY, geom CIRCLE);
INSERT INTO circle_1 VALUES (1, ARRAY[circle '((0,0),5)',circle '((0,0),5)']);
INSERT INTO circle_1 VALUES (2, ARRAY[NULL::circle,NULL::circle]);

-- CREATE TABLE path_1(gid serial PRIMARY KEY, geom PATH);
INSERT INTO path_1 VALUES (1, ARRAY['[(0,0),(0,1),(1,1),(1,0),(0,0)]'::path, '[(0,0),(0,1),(1,1),(1,0),(0,0)]'::path]);
INSERT INTO path_1 VALUES (2, ARRAY[NULL::path,NULL::path]);

-- CREATE TABLE point_1(gid serial PRIMARY KEY, geom POINT);
INSERT INTO point_1 VALUES (1, ARRAY['(1.0,1.0)'::POINT,'(1.0,1.0)'::POINT]);
INSERT INTO point_1 VALUES (2, ARRAY[NULL::POINT,NULL::POINT]);

-- CREATE TABLE polygon_1(gid serial PRIMARY KEY, geom POLYGON);
INSERT INTO polygon_1 VALUES (1, ARRAY['((0.0,0.0),(0.0,1.0),(1.0,1.0))'::POLYGON,'((0.0,0.0),(0.0,1.0),(1.0,1.0))'::POLYGON]);
INSERT INTO polygon_1 VALUES (2, ARRAY[NULL::POLYGON,NULL::POLYGON]);

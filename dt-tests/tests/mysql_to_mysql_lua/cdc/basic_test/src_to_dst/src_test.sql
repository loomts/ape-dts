INSERT INTO lua_test.add_column_test VALUES(1,1);
INSERT INTO lua_test.change_column_value_test VALUES(1,1);
INSERT INTO lua_test.drop_column_test VALUES(1,1,1);
INSERT INTO lua_test.change_column_name_test VALUES(1,1);
INSERT INTO lua_test.change_table_name_test VALUES(1,1);
INSERT INTO lua_test.filter_row_test VALUES(1,1),(2,2);

INSERT INTO lua_test.change_string_column_value_test VALUES(1, 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀');
INSERT INTO lua_test.change_string_column_value_test VALUES(2, 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀');

INSERT INTO lua_test.filter_blob_column_value_test VALUES(1, x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789');
INSERT INTO lua_test.filter_blob_column_value_test VALUES(2, x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789');

UPDATE lua_test.add_column_test SET f_1 = 2000;
UPDATE lua_test.change_column_value_test SET f_1 = 2000;
UPDATE lua_test.drop_column_test SET f_1 = 2000, f_2 = 2000;
UPDATE lua_test.change_column_name_test SET f_1 = 2000;
UPDATE lua_test.change_table_name_test SET f_1 = 2000;
UPDATE lua_test.filter_row_test SET f_1 = 2000;

UPDATE lua_test.change_string_column_value_test SET f_1 = 'b中文😀', f_2 = 'b中文😀', f_3 = 'b中文😀', f_4 = 'b中文😀', f_5 = 'b中文😀', f_6 = 'b中文😀';

UPDATE lua_test.filter_blob_column_value_test SET f_1 = x'0123456789ABCDEF', f_2 = x'0123456789ABCDEF', f_3 = x'0123456789ABCDEF', f_4 = x'0123456789ABCDEF', f_5 = x'0123456789ABCDEF', f_6 = x'0123456789ABCDEF';

DELETE FROM lua_test.add_column_test;
DELETE FROM lua_test.change_column_value_test;
DELETE FROM lua_test.drop_column_test;
DELETE FROM lua_test.change_column_name_test;
DELETE FROM lua_test.change_table_name_test;
DELETE FROM lua_test.filter_row_test;
DELETE FROM lua_test.change_string_column_value_test;
DELETE FROM lua_test.filter_blob_column_value_test;
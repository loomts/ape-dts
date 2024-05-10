INSERT INTO lua_test_expect.add_column_test VALUES(1,1,1000);
INSERT INTO lua_test_expect.change_column_value_test VALUES(1,1000);
INSERT INTO lua_test_expect.drop_column_test VALUES(1,1);
INSERT INTO lua_test_expect.change_column_name_test VALUES(1,1);
INSERT INTO lua_test_expect.change_table_name_test_dst VALUES(1,1);
INSERT INTO lua_test_expect.filter_row_test VALUES(2,2);

INSERT INTO lua_test_expect.change_string_column_value_test VALUES(1, '😀中文a', '😀中文a', '😀中文a', '😀中文a', '😀中文a', '😀中文a');
INSERT INTO lua_test_expect.change_string_column_value_test VALUES(2, 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀', 'a中文😀');

INSERT INTO lua_test_expect.filter_blob_column_value_test VALUES(1, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO lua_test_expect.filter_blob_column_value_test VALUES(2, x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789', x'ABCDEF0123456789');

UPDATE lua_test_expect.add_column_test SET f_1 = 2000;
UPDATE lua_test_expect.change_column_value_test SET f_1 = 1000;
UPDATE lua_test_expect.drop_column_test SET f_1 = 2000;
UPDATE lua_test_expect.change_column_name_test SET f_1_1 = 2000;
UPDATE lua_test_expect.change_table_name_test_dst SET f_1 = 2000;
UPDATE lua_test_expect.filter_row_test SET f_1 = 2000 where id = 2;

UPDATE lua_test_expect.change_string_column_value_test SET f_1 = 'b中文😀', f_2 = 'b中文😀', f_3 = 'b中文😀', f_4 = 'b中文😀', f_5 = 'b中文😀', f_6 = 'b中文😀' WHERE id = 2;

UPDATE lua_test_expect.filter_blob_column_value_test SET f_1 = x'0123456789ABCDEF', f_2 = x'0123456789ABCDEF', f_3 = x'0123456789ABCDEF', f_4 = x'0123456789ABCDEF', f_5 = x'0123456789ABCDEF', f_6 = x'0123456789ABCDEF' WHERE id = 2;

DELETE FROM lua_test_expect.add_column_test;
DELETE FROM lua_test_expect.change_column_value_test;
DELETE FROM lua_test_expect.drop_column_test;
DELETE FROM lua_test_expect.change_column_name_test;
DELETE FROM lua_test_expect.change_table_name_test_dst;
DELETE FROM lua_test_expect.filter_row_test WHERE id = 2;
DELETE FROM lua_test_expect.change_string_column_value_test;
DELETE FROM lua_test_expect.filter_blob_column_value_test;
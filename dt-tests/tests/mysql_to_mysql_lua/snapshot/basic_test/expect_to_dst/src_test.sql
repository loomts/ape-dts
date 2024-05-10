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

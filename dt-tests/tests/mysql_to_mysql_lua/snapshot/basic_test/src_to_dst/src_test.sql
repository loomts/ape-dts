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

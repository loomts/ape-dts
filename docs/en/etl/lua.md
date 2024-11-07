# Modify data by Lua

During data sync, a user may need to modify the data manually.

In ape-dts tasks, the user can provide a Lua script to process each row data, such as:

- Add columns
- Drop columns
- Modify column values
- Modify schema name / table name / column name
- Modify row type, eg: change update data to insert data
- Filter rows

# Config
- in task_config.ini
```
[processor]
lua_code_file=./dt-tests/tests/mysql_to_mysql_lua/cdc/basic_test/src_to_dst/lua_code.lua
```

# How it works
The task passes each row data pulled by the extractor into Lua as global variables, and then executes user code to process the data.

Processed data will continue subsequent processes of the task.


- Global variables passed into Lua:

| Variable | Data Type | Description |
| :-------- | :-------- | :-------- |
| schema | string | database name(mysql) / schema name(postgres) |
| tb | string | table name |
| row_type | string | row data type: insert / update / delete |
| before | table | exists in update / delete row data, key for column name，value for column value |
| after | table | exists in  update / insert row data, key for column name，value for column value |

# Examples
## Add columns
```
if (schema == "lua_test" and tb == "add_column_test" and row_type == "insert")
then
    after["new_column"] = 1000
end
```

## Drop columns
```
if (schema == "lua_test" and tb == "drop_column_test" and row_type == "insert")
then
    after["column_1"] = nil
end
```

## Modify column values
```
if (schema == "lua_test" and tb == "change_column_value_test" and row_type == "insert")
then
    after["column_1"] = "new_value"
end
```

## Change column name
```
if (schema == "lua_test" and tb == "change_column_name_test" and row_type == "insert")
then
    after["f_1_1"] = after.f_1
    after["f_1"] = nil
end
```

## Change table name
```
if (schema == "lua_test" and tb == "change_table_name_test")
then
    tb = "change_table_name_test_dst"
end
```

## Filter rows
- set row_type to "", the row will be filtered
```
if (schema == "lua_test" and tb == "filter_row_test")
then
    if (before.id == 1 or after.id == 1)
    then
        row_type = ""
    end
end
```

## More references
- test cases: dt-tests/tests/mysql_to_mysql_lua, dt-tests/tests/pg_to_pg_lua

# Supported scenarios
- Only for snapshot / cdc tasks whose source is mysql / postgres.
- For cdc tasks, only supports processing on dml data.
- For binary columns, eg: mysql tinyblob, mediumblob, longblob, blob, varbinary, binary, currently:
    - Dropping these columns is supported.
    - Modifying them is NOT supported.

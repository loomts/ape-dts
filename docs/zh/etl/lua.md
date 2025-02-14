# 使用 Lua 加工数据
在数据同步中，用户可能需要对数据做简单加工。

执行 ape-dts 任务时，用户可提供一份 Lua 代码，用以对每条数据进行加工，如：

- 添加列
- 丢弃列
- 修改列值
- 修改库名 / 表名 / 列名
- 修改数据类型，如：将 update 数据改成 insert
- 过滤数据

# 配置
- 在 task_config.ini 中添加：
```
[processor]
lua_code_file=./dt-tests/tests/mysql_to_mysql_lua/cdc/basic_test/src_to_dst/lua_code.lua
```

# 原理
ape-dts 将 extractor 拉取到的每条数据作为全局变量传入 Lua，并执行用户代码以加工数据。

加工后的数据会继续执行该任务后续流程。

- 传入 Lua 的全局变量：

| 变量 | 数据类型 | 含义 |
| :-------- | :-------- | :-------- |
| schema | string | 库名(mysql) / schema 名(postgres) |
| tb | string | 表名 |
| row_type | string | 数据类型：insert / update / delete |
| before | table | 存在于 update / delete 数据中，key 为字段名，value 为字段值 |
| after | table | 存在于 update / insert 数据中，key 为字段名，value 为字段值 |


# 示例
## 添加列
```
if (schema == "lua_test" and tb == "add_column_test" and row_type == "insert")
then
    after["new_column"] = 1000
end
```

## 丢弃列
```
if (schema == "lua_test" and tb == "drop_column_test" and row_type == "insert")
then
    after["column_1"] = nil
end
```

## 修改列值
```
if (schema == "lua_test" and tb == "change_column_value_test" and row_type == "insert")
then
    after["column_1"] = "new_value"
end
```

## 修改列名
```
if (schema == "lua_test" and tb == "change_column_name_test" and row_type == "insert")
then
    after["f_1_1"] = after.f_1
    after["f_1"] = nil
end
```

## 修改表名
```
if (schema == "lua_test" and tb == "change_table_name_test")
then
    tb = "change_table_name_test_dst"
end
```

## 过滤数据
- 将 row_type 设置成 ""，该数据将会被丢弃。
```
if schema == "lua_test" and tb == "filter_row_test" then
    if (after.create_time ~= nil and after.create_time < '2024-12-01 00:00:00') or
       (before.create_time ~= nil and before.create_time < '2024-12-01 00:00:00') then
        row_type = ""
    end
end
```

## 更多参考
- 请参考测试用例：dt-tests/tests/mysql_to_mysql_lua, dt-tests/tests/pg_to_pg_lua

# 支持范围
- 仅支持源端为 mysql / postgres 的 snapshot / cdc 任务。
- 对于 cdc 任务，仅支持对 dml 数据进行加工。
- 对于二进制数据列，如：mysql 的 tinyblob，mediumblob，longblob，blob，varbinary，binary，目前仅支持对该列过滤，不支持对其修改。



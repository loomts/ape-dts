
-- add column f_2
if (schema == "lua_test" and tb == "add_column_test")
then
    after.f_2 = 1000
end

-- drop column f_2
if (schema == "lua_test" and tb == "drop_column_test")
then
    after.f_2 = nil
end

-- change value of column f_1
if (schema == "lua_test" and tb == "change_column_value_test")
then
    after.f_1 = 1000
end

-- change value of string columns
if (schema == "lua_test" and tb == "change_string_column_value_test")
then
    if (before.id == 1 or after.id == 1)
    then
        after.f_1 = "😀中文a"
        after.f_2 = "😀中文a"
        after.f_3 = "😀中文a"
        after.f_4 = "😀中文a"
        after.f_5 = "😀中文a"
        after.f_6 = "😀中文a"
    end
end

-- filter value of blob columns
if (schema == "lua_test" and tb == "filter_blob_column_value_test")
then
    if (before.id == 1 or after.id == 1)
    then
        after.f_1 = nil
        after.f_2 = nil
        after.f_3 = nil
        after.f_4 = nil
        after.f_5 = nil
        after.f_6 = nil
    end
end

-- change column name f_1 to f_1_1
if (schema == "lua_test" and tb == "change_column_name_test")
then
    after.f_1_1 = after.f_1
    after.f_1 = nil
end

-- change table name 
if (schema == "lua_test" and tb == "change_table_name_test")
then
    tb = "change_table_name_test_dst"
end

-- filter row
if (schema == "lua_test" and tb == "filter_row_test")
then
    if (before.id == 1 or after.id == 1)
    then
        row_type = ""
    end
end

print("schema: "..schema)
print("tb: "..tb)
print("row_type: "..row_type)

print("")
print("before")
for k, v in pairs(before) do
    print(k, v)
end

print("")
print("after")
for k, v in pairs(after) do
    print(k, v)
end
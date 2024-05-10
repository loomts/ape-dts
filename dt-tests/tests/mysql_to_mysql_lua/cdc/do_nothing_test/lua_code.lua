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

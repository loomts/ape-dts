-- JSON
JSON.SET json-1 $ '{"name":"John", "age":30, "city":"New York"}'
JSON.SET json-2 $ '{"products":[{"id":1,"name":"Apple"},{"id":2,"name":"Banana"}]}'
JSON.SET json-3 $ '[1,2,3,4,5]'
JSON.DEL json-2 $.products[0]
JSON.STRAPPEND json-1 $.name '"son"'
JSON.NUMINCRBY json-1 $.age 5
JSON.ARRAPPEND json-3 $ 6 7 8
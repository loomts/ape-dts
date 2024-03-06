-- JSON.SET
JSON.SET 1-1 $ '{"a":2}'
JSON.SET 1-1 $.b '8'
-- JSON.GET 1-1 $

JSON.SET 2-1 $ '{"f1": {"a":1}, "f2":{"a":2}}'
JSON.SET 2-1 $..a 3

-- JSON.ARRAPPEND
JSON.SET 3-1 $ '{"price":99.98,"stock":25,"colors":["black","silver"]}'
JSON.ARRAPPEND 3-1 $.colors '"blue"'

-- JSON.ARRINDEX
JSON.SET 4-1 $ '{"price":99.98,"stock":25,"colors":["black","silver"]}'
JSON.ARRINDEX 4-1 $..colors '"silver"'

-- JSON.ARRINSERT
JSON.SET 5-1 $ '{"price":99.98,"stock":25,"colors":["black","silver"]}'
JSON.ARRINSERT 5-1 $.colors 2 '"yellow"' '"gold"'

-- JSON.ARRPOP
JSON.SET 6-1 $ '[{"name":"Healthy headphones","description":"Wireless Bluetooth headphones with noise-cancelling technology","connection":{"wireless":true,"type":"Bluetooth"},"price":99.98,"stock":25,"colors":["black","silver"],"max_level":[60,70,80]},{"name":"Noisy headphones","description":"Wireless Bluetooth headphones with noise-cancelling technology","connection":{"wireless":true,"type":"Bluetooth"},"price":99.98,"stock":25,"colors":["black","silver"],"max_level":[80,90,100,120]}]'
JSON.ARRPOP 6-1 $.[1].max_level 0

-- -- JSON.ARRTRIM
JSON.SET 7-1 $ "[[{\"name\":\"Healthy-headphones\",\"description\":\"Wireless-Bluetooth-headphones-with-noise-cancelling-technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"],\"max_level\":[60,70,80]},{\"name\":\"Noisy-headphones\",\"description\":\"Wireless-Bluetooth-headphones-with-noise-cancelling-technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"],\"max_level\":[85,90,100,120]}]]"
JSON.ARRAPPEND 7-1 $.[1].max_level 140 160 180 200 220 240 260 280
JSON.ARRTRIM 7-1 $.[1].max_level 4 8

-- JSON.CLEAR
JSON.SET 8-1 $ '{"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14}'
JSON.CLEAR 8-1 $.*

-- JSON.DEL
JSON.SET 9-1 $ '{"a": 1, "nested": {"a": 2, "b": 3}}'
JSON.DEL 9-1 $..a

-- JSON.FORGET 
JSON.SET 10-1 $ '{"a": 1, "nested": {"a": 2, "b": 3}}'
JSON.FORGET 10-1 $..a

-- JSON.MERGE
-- Create a unexistent path-value
JSON.SET 11-1 $ '{"a":2}'
JSON.MERGE 11-1 $.b '8'
-- Delete on existing value
JSON.SET 11-2 $ '{"a":2}'
JSON.MERGE 11-2 $.a 'null'
-- Replace an Array
JSON.SET 11-3 $ '{"a":[2,4,6,8]}'
JSON.MERGE 11-3 $.a '[10,12]'

-- JSON.MSET
JSON.MSET 12-2 $ '{"a":2}'
JSON.MSET 12-3 $ '{"a":2}'
JSON.MSET 12-1 $ '{"a":2}' 12-2 $.f.a '3' 12-3 $ '{"f1": {"a":1}, "f2":{"a":2}}'

-- JSON.NUMINCRBY
JSON.SET 13-1 . '{"a":"b","b":[{"a":2}, {"a":5}, {"a":"c"}]}'
JSON.NUMINCRBY 13-1 $.a 2
JSON.NUMINCRBY 13-1 $..a 2

-- JSON.NUMMULTBY
JSON.SET 14-1 . '{"a":"b","b":[{"a":2}, {"a":5}, {"a":"c"}]}'
JSON.NUMMULTBY 14-1 $.a 2
JSON.NUMMULTBY 14-1 $..a 2

-- JSON.STRAPPEND
JSON.SET 15-1 $ '{"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31}}'
JSON.STRAPPEND 15-1 $..a '"baz"'

-- JSON.TOGGLE 
JSON.SET 16-1 $ '{"bool": true}'
JSON.TOGGLE 16-1 $.bool

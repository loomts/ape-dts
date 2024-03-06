SET set_key_3_1 val_1
SET set_key_3_2_中文 val_2_中文
SET "set_key_3_3_  😀" "val_2_  😀"

SELECT 2

HSET hset_key_3_1 field_1 val_1

SELECT 3

HMSET hmset_key_3_1 field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"

SELECT 4

LPUSH list_key_3_1 val_1 
RPUSH list_key_3_1 val_2 val_3
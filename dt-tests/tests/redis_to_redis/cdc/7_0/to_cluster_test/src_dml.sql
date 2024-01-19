-------------------- add entries --------------------

-------------------- string entries
-- SET
SET set_key_1 val_1
SET set_key_2_中文 val_2_中文
SET "set_key_3_  😀" "val_2_  😀"

-- MSET
-- MSET mset_key_1 val_1 mset_key_2_中文 val_2_中文 "mset_key_3_  😀" "val_3_  😀"

-------------------- hash entries
-- HSET
HSET hset_key_1 field_1 val_1
HSET hset_key_1 field_2_中文 val_2_中文
HSET hset_key_1 "field_3_  😀" "val_3_  😀"

-- HMSET
HMSET hmset_key_1 field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"

-------------------- list entries
-- LPUSH
LPUSH list_key_1 val_1 
LPUSH list_key_1 val_2_中文
LPUSH list_key_1 "val_3_  😀"

-- RPUSH
RPUSH list_key_1 val_5 val_6  

-- LINSERT
LINSERT list_key_1 BEFORE val_1 val_7

-------------------- sets entries
-- SADD
SADD sets_key_1 val_1 val_2_中文 "val_3_  😀" val_5

-- SREM
SREM sets_key_1 val_5 

-------------------- zset entries
-- ZADD
ZADD zset_key_1 1 val_1 2 val_2_中文 3 "val_3_  😀"
ZINCRBY zset_key_1 5 val_1 

-------------------- stream entries
-- XADD
XADD stream_key_1 * field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"
XADD "stream_key_2  中文😀" * field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"


-------------------- remove entries --------------------

-------------------- string entries
-- DEL
DEL "set_key_3_  😀" 

DEL mset_key_2_中文 "mset_key_3_  😀"

-------------------- hash entries
-- HDEL
HDEL hset_key_1 "field_3_  😀"

-- HMDEL
HDEL hmset_key_1 field_2_中文 "field_3_  😀"

-------------------- list entries
-- LPOP
LPOP list_key_1 

-- LTRIM
LTRIM list_key_1 0 2

-- RPOP
RPOP list_key_1

-------------------- sets entries
SREM sets_key_1 val_2_中文 "val_3_  😀"

-------------------- zset entries
ZREM zset_key_1 val_1 

-------------------- stream entries
XTRIM stream_key_1 MAXLEN 0
DEL "stream_key_2  中文😀"
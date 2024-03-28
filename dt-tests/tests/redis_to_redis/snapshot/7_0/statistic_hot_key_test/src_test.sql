-------------------- string entries
-- SET
SET "set_key_1_  😀" "val_2_  😀"
SET "set_key_2_  😀" "val_2_  😀"

GET "set_key_1_  😀"

-- MSET
MSET mset_key_1 val_1 mset_key_2_中文 val_2_中文 "mset_key_3_  😀" "val_3_  😀"

GET mset_key_1

-------------------- hash entries
-- HSET
HSET hset_key_1 "field_3_  😀" "val_3_  😀"
HSET hset_key_2 "field_3_  😀" "val_3_  😀"

HGETALL hset_key_1

-- HMSET
HMSET hmset_key_1 field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"
HMSET hmset_key_2 field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"

HGETALL hmset_key_1

-------------------- list entries
-- LPUSH
LPUSH list_key_1 "val_3_  😀"
LPUSH list_key_2 "val_3_  😀"

LRANGE list_key_1 0 -1

-------------------- sets entries
-- SADD
SADD sets_key_1 val_1 val_2_中文 "val_3_  😀" val_5
SADD sets_key_2 val_1 val_2_中文 "val_3_  😀" val_5

SORT sets_key_1 ALPHA

-------------------- zset entries
-- ZADD
ZADD zset_key_1 1 val_1 2 val_2_中文 3 "val_3_  😀"
ZADD zset_key_2 1 val_1 2 val_2_中文 3 "val_3_  😀"

ZRANGE zset_key_1 0 -1 WITHSCORES

-------------------- stream entries
-- XADD
XADD "stream_key_1  中文😀" * field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"
XADD "stream_key_2  中文😀" * field_1 val_1 field_2_中文 val_2_中文 "field_3_  😀" "val_3_  😀"

XRANGE "stream_key_1  中文😀" - +
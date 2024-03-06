-- FT.ALIASADD
FT.CREATE 1-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
FT.ALIASADD alias-1-1 1-1
-- FT._LIST
-- FT.INFO 1-1

-- FT.ALIASDEL
FT.CREATE 2-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT
FT.ALIASADD alias-2-1 2-1
FT.ALIASDEL alias-2-1

-- FT.ALIASUPDATE
FT.CREATE 3-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT
FT.ALIASADD alias-3-1 3-1
FT.ALIASUPDATE alias-3-2 3-1

-- FT.ALTER
FT.CREATE 4-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT
FT.ALTER 4-1 SCHEMA ADD id2 NUMERIC SORTABLE

-- FT.CREATE
-- Create an index that stores the title, publication date, and categories of blog post hashes whose keys start with blog:post: (for example, blog:post:1).
FT.CREATE 5-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
-- Index the sku attribute from a hash as both a TAG and as TEXT:
FT.CREATE 5-2 ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE
-- Index two different hashes, one containing author data and one containing books, in the same index:
FT.CREATE 5-3 ON HASH PREFIX 2 author:details: book:details: SCHEMA author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
-- Index authors whose names start with G.
FT.CREATE 5-4 ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
-- Index only books that have a subtitle.
FT.CREATE 5-5 ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
-- Index books that have a "categories" attribute where each category is separated by a ; character.
FT.CREATE 5-6 ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT categories TAG SEPARATOR ;
-- Index a JSON document using a JSON Path expression.
FT.CREATE 5-7 ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG

-- FT.DROPINDEX
FT.CREATE 6-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT
FT.DROPINDEX 6-1 DD

-- FT.SYNUPDATE
FT.CREATE 7-1 ON HASH PREFIX 1 blog:post: SCHEMA title TEXT
FT.SYNUPDATE 7-1 synonym hello hi shalom

-- -- FT.DICTADD
-- FT.DICTADD 7-1 foo bar "hello world"

-- FT.PROFILE idx SEARCH QUERY "hello world"

-- FT.SUGADD sug "hello world" 1
-- FT.SUGGET

-- FT.SUGDEL
-- FT.SUGDEL sug "hello"



INSERT INTO tb_col_utf8 VALUES(1, 'abc');
INSERT INTO tb_col_utf8 VALUES(2, '中文');
INSERT INTO tb_col_utf8 VALUES(3, 'わたし');
INSERT INTO tb_col_utf8 VALUES(4, '대한민국');
INSERT INTO tb_col_utf8 VALUES(5, '😀');
INSERT INTO tb_col_utf8 VALUES(6, NULL);

UPDATE tb_col_utf8 set value = (select value from tb_col_utf8 where pk = 5) WHERE pk = 6;
UPDATE tb_col_utf8 set value = (select value from tb_col_utf8 where pk = 4) WHERE pk = 5;
UPDATE tb_col_utf8 set value = (select value from tb_col_utf8 where pk = 3) WHERE pk = 4;
UPDATE tb_col_utf8 set value = (select value from tb_col_utf8 where pk = 2) WHERE pk = 3;
UPDATE tb_col_utf8 set value = (select value from tb_col_utf8 where pk = 1) WHERE pk = 2;
UPDATE tb_col_utf8 set value = NULL WHERE pk = 1;

DELETE FROM tb_col_utf8;
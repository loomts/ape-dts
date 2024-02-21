INSERT INTO tb_col_euc_cn VALUES(1, 'abc');
INSERT INTO tb_col_euc_cn VALUES(2, '中文');
INSERT INTO tb_col_euc_cn VALUES(3, NULL);

UPDATE tb_col_euc_cn set value = (select value from tb_col_euc_cn where pk = 2) WHERE pk = 3;
UPDATE tb_col_euc_cn set value = (select value from tb_col_euc_cn where pk = 1) WHERE pk = 2;
UPDATE tb_col_euc_cn set value = NULL WHERE pk = 1;

DELETE FROM tb_col_euc_cn;
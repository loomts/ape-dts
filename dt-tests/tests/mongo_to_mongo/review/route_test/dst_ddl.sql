use dst_test_db_1

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");

use dst_test_db_2

db.dropDatabase();

db.createCollection("dst_tb_1");

use test_db_2

db.dropDatabase();

db.createCollection("tb_2");
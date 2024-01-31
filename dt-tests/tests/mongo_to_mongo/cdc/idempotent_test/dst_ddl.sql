use test_db_1

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");

use test_db_2

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");

use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1000", "_id": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2000", "_id": "2" });
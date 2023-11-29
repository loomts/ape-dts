use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });
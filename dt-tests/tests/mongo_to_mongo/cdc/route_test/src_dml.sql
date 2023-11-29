use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });

db.tb_1.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_1.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.tb_1.deleteOne({ "name": "a_1", "age": "1" });
db.tb_1.deleteOne({ "name": "b_1", "age": "2" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });

db.tb_2.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_2.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.tb_2.deleteOne({ "name": "a_1", "age": "1" });
db.tb_2.deleteOne({ "name": "b_1", "age": "2" });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });

db.tb_1.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_1.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.tb_1.deleteOne({ "name": "a_1", "age": "1" });
db.tb_1.deleteOne({ "name": "b_1", "age": "2" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });

db.tb_2.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_2.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.tb_2.deleteOne({ "name": "a_1", "age": "1" });
db.tb_2.deleteOne({ "name": "b_1", "age": "2" });
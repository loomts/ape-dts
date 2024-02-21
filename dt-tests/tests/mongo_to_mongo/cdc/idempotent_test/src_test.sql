use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1", "_id": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2", "_id": "2" });
db.tb_1.insertOne({ "name": "c", "age": "3" });
db.tb_1.insertOne({ "name": "d", "age": "4" });
db.tb_1.insertOne({ "name": "e", "age": "5" });

db.tb_1.deleteOne({ "name": "a", "age": "1" });
db.tb_1.updateOne({ "age" : "2" }, { "$set": { "name" : "d_1" } });
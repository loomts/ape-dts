use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" })
db.tb_1.insertOne({ "name": "b", "age": "2" })
db.tb_1.insertOne({ "name": "c", "age": "3" })
db.tb_1.insertOne({ "name": "d", "age": "4" })
db.tb_1.insertOne({ "name": "d", "age": "5" })

db.tb_2.insertOne({ "name": "a", "age": "1" })
db.tb_2.insertOne({ "name": "b", "age": "2" })
db.tb_2.insertOne({ "name": "c", "age": "3" })
db.tb_2.insertOne({ "name": "d", "age": "4" })
db.tb_2.insertOne({ "name": "d", "age": "5" })
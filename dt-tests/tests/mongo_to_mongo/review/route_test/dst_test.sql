use dst_test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" , "_id": "65733a82fb2ce9836745de4a" });
db.tb_1.insertOne({ "name": "b", "age": "2" , "_id": "65733a82fb2ce9836745de4b" });
db.tb_1.insertOne({ "name": "c", "age": "3000" , "_id": "65733a82fb2ce9836745de4c" });
db.tb_1.insertOne({ "name": "d", "age": "4" , "_id": "65733a82fb2ce9836745de4d" });

db.tb_2.insertOne({ "name": "a", "age": "1" , "_id": "65733a82fb2ce9836745de4f" });
db.tb_2.insertOne({ "name": "c", "age": "3" , "_id": "65733a82fb2ce9836745de4h" });
db.tb_2.insertOne({ "name": "d", "age": "4000" , "_id": "65733a82fb2ce9836745de4i" });
db.tb_2.insertOne({ "name": "e", "age": "5000" , "_id": "65733a82fb2ce9836745de4j" });

use dst_test_db_2

db.dst_tb_1.insertOne({ "name": "a", "age": "1", "_id": "65733a82fb2ce9836745de4k" }); 
db.dst_tb_1.insertOne({ "name": "b", "age": "2", "_id": "65733a82fb2ce9836745de4l" });
db.dst_tb_1.insertOne({ "name": "a", "age": "3", "_id": "65733a82fb2ce9836745de4m" });
db.dst_tb_1.insertOne({ "name": "b", "age": "4", "_id": "65733a82fb2ce9836745de4n" }); 
db.dst_tb_1.insertOne({ "name": "b", "age": "5000", "_id": "65733a82fb2ce9836745de4o" }); 

use test_db_2
db.tb_2.insertOne({ "name": "a", "age": "1", "_id": "65733a82fb2ce9836745de4p" }); 
db.tb_2.insertOne({ "name": "a", "age": "3", "_id": "65733a82fb2ce9836745de4r" });
db.tb_2.insertOne({ "name": "b", "age": "4", "_id": "65733a82fb2ce9836745de4s" }); 
db.tb_2.insertOne({ "name": "b", "age": "5000", "_id": "65733a82fb2ce9836745de4t" }); 

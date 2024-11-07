# Benchmark

# MySQL -> MySQL

Use [sysbench](https://github.com/akopytov/sysbench) to generate snapshot data and binlogs.

Run tests with [ape_dts](/docs/en/tutorial/mysql_to_mysql.md) and [debezium](https://github.com/debezium/debezium).

## Test Environment

The source database, target database, and data migration task are located on 3 Baidu BCC machines within the same network, each with a specification of 8c16g.

## MySQL Specifications

| | Source | Target |
| :-------- | :-------- | :-------- | 
| Version | mysql:8.2.0| mysql:8.2.0 |
| Specs | 8c16g| 8c16g |

## Snapshot migration

Generate 10 tables through sysbench oltp_read_write, each with 5,000,000 records.

```
sysbench oltp_read_write --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=10 prepare
```

### Results
| Method | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :-------- | :-------- | :-------- | :-------- | :-------- | 
| ape_dts | 1c2g | 71428 | 8.2% / 5.2% | 211% / 5.1% |
| ape_dts | 2c4g | 99403 | 14.0% / 5.2% | 359% / 5.1% |
| ape_dts | 4c8g | 126582 | 13.8% / 5.2% | 552% / 5.1% |
| debezium | 4c8g |	4051 | 21.5% / 5.2% | 51.2% / 5.1% |

- According to [debezium official](https://debezium.io/blog/2023/12/20/JDBC-sink-connector-batch-support/): RPS varies between 2000 and 6000 depending on batch.size and the storage format of data in Kafka.

## CDC synchronization
### Case 1: Updates on 10 tables

Generate binlogs on 10 tables through sysbench oltp_update_index, about 3,200,000 records.

```
sysbench oltp_update_index --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=100 --time=1200 --report-interval=10 run
```

#### Results
| Method | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 11902 | 19.0% / 5.2% | 479% / 5.1% |
| ape_dts | 2c4g | 14240 | 18.6% / 5.2% | 623% / 5.1% |
| ape_dts | 4c8g | 19450 | 19.2% / 5.2% | 689% / 5.1% |
| debezium | 4c8g | 3175 | 17.9% / 5.2% | 118% / 5.1% |

### Case 2: Updates on 1 table

Generate binlogs on 1 table through sysbench oltp_update_index, about 3,200,000 records.

```
sysbench oltp_update_index --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=100 --time=1200 --report-interval=1 run
```

#### Results
| Method | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 15002 | 18.8% / 5.2% | 467% / 6.5% | 
| ape_dts | 2c4g | 24692 | 18.1% / 5.2% | 687% / 6.5% | 
| ape_dts | 4c8g | 26287 | 18.2% / 5.2% | 685% / 6.5% |
| debezium | 4c8g | 2951 | 20.4% / 5.2% | 98% / 6.5% |
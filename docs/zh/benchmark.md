# Benchmark

# MySQL -> MySQL

使用 [sysbench](https://github.com/akopytov/sysbench) 生成全量和增量数据。

分别使用 ape_dts 和 [debezium](https://github.com/debezium/debezium) 执行迁移任务，并对比结果。

## 测试环境
源库，目标库，数据迁移任务分别位于同一网络的 3 台百度 BCC 机器，规格 8c16g。

## MySQL 规格

| | 源库 | 目标库 |
| :-------- | :-------- | :-------- | 
| 版本 | mysql:8.2.0| mysql:8.2.0 |
| 规格 | 8c16g| 8c16g |

## 全量迁移
使用 sysbench oltp_read_write 生成 10 张表，每张表 500万条数据。

```
sysbench oltp_read_write --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=10 prepare
```

### 结果
| 同步方式 | 节点规格 | rps（rows per second) |	源 MySQL 负荷（cpu/内存） | 目标 MySQL 负荷（cpu/内存） |
| :-------- | :-------- | :-------- | :-------- | :-------- | 
| ape_dts | 1c2g | 71428 | 8.2% / 5.2% | 211% / 5.1% |
| ape_dts | 2c4g | 99403 | 14.0% / 5.2% | 359% / 5.1% |
| ape_dts | 4c8g | 126582 | 13.8% / 5.2% | 552% / 5.1% |
| debezium | 4c8g |	4051 | 21.5% / 5.2% | 51.2% / 5.1% |

- 注：debezium [官方数据](https://debezium.io/blog/2023/12/20/JDBC-sink-connector-batch-support/)：根据 batch.size 和数据在 kafka 上的存储格式不同，rps 在 2000 到 6000 之间

## 增量迁移
### 测试 1: 10 张表并行 update
使用 sysbench oltp_update_index 生成约 320万条增量数据。
```
sysbench oltp_update_index --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=100 --time=1200 --report-interval=10 run
```

#### 结果
| 同步方式 | 节点规格 | rps（rows per second) | 源 MySQL 负荷（cpu/内存） | 目标 MySQL 负荷（cpu/内存） |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 11902 | 19.0% / 5.2% | 479% / 5.1% |
| ape_dts | 2c4g | 14240 | 18.6% / 5.2% | 623% / 5.1% |
| ape_dts | 4c8g | 19450 | 19.2% / 5.2% | 689% / 5.1% |
| debezium | 4c8g | 3175 | 17.9% / 5.2% | 118% / 5.1% |

### 测试 2：1 张表 update
使用 sysbench oltp_update_index 生成约 320万条增量数据。
```
sysbench oltp_update_index --mysql-host=192.168.80.3 --mysql-port=3307 --mysql-user=root --mysql-password=123456 --mysql-db=sbtest --tables=10 --table-size=5000000 --threads=100 --time=1200 --report-interval=1 run
```

#### 结果
| 同步方式 | 节点规格 | rps（rows per second) | 源 MySQL 负荷（cpu/内存） | 目标 MySQL 负荷（cpu/内存） |
| :-------- | :-------- | :-------- | :-------- | :-------- |
| ape_dts | 1c2g | 15002 | 18.8% / 5.2% | 467% / 6.5% | 
| ape_dts | 2c4g | 24692 | 18.1% / 5.2% | 687% / 6.5% | 
| ape_dts | 4c8g | 26287 | 18.2% / 5.2% | 685% / 6.5% |
| debezium | 4c8g | 2951 | 20.4% / 5.2% | 98% / 6.5% |
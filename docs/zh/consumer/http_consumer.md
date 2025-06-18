## Unstable Features

> ⚠️ **Stability Warning**: Unstable features may change or be removed without notice. They are not subject to semantic versioning guarantees.

# 启动 ape_dts 为 HTTP server 并提供数据给消费者

参考 [教程](/docs/en/tutorial/mysql_to_http_server_consumer.md)

ape_dts 启动为 HTTP server，根据用户配置，拉取 MySQL/Postgres 的全量/增量数据，缓存在内存中。

用户通过 api 从 ape_dts 拉取数据并消费，数据格式为 avro，与 [ape_dts 发送到 Kafka](/docs/zh/consumer/kafka_consumer.md) 的数据格式一致。

# Api

## info

获取 server 当前信息。

curl "http://127.0.0.1:10231/info"

### 返回值

```
{"acked_batch_id":0,"sent_batch_id":0}
```

- batch_id：由 ape_dts 生成，用户每拉取一次数据，自增 1，初始值为 0，ape_dts 启动时重置。
- sent_batch_id：已经发送到客户端的最大 batch_id。
- acked_batch_id：客户端已确认消费的最大 batch_id，确认过的数据将从 ape_dts 的缓存中清除。

## fetch_new

从 server 获取新数据。

curl "http://127.0.0.1:10231/fetch_new?batch_size=2&ack_batch_id=1"

### 参数

- batch_size：一次最多拉取数据条数，如果 ape_dts 缓存不足，则返回全部数据。
- ack_batch_id：可选。
  - 如果设置，表示在拉取新数据时，通知 ape_dts 将 ack_batch_id 及之前的数据标为消费完成。
  - ack_batch_id 必须 >= info 返回的 acked_batch_id。
  - ack_batch_id 必须 <= info 返回的 sent_batch_id。

### 返回值

```
{"data":[[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,2,10,118,97,108,117,101,4,2,0,0],[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,4,10,118,97,108,117,101,4,4,0,0]],"batch_id":1}
```

- data：多条数据，二进制，以 avro 格式编码，参考本文后面的 [解析及消费]。
- batch_id：ape_dts 为本次拉取生成的 id。

## fetch_old

从 server 重复获取旧数据。

curl "http://127.0.0.1:10232/fetch_old?old_batch_id=1"

### 参数

- old_batch_id：要获取的旧数据的 batch_id。
  - old_batch_id 必须 <= info 返回的 sent_batch_id。
  - old_batch_id 必须 > info 返回的 acked_batch_id，因为小于 acked_batch_id 的数据已被 ape_dts 从缓存中清除。

### 返回值

```
{"data":[[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,2,10,118,97,108,117,101,4,2,0,0],[14,116,101,115,116,95,100,98,8,116,98,95,49,12,105,110,115,101,114,116,2,4,4,105,100,6,105,110,116,8,76,111,110,103,10,118,97,108,117,101,6,105,110,116,8,76,111,110,103,0,0,2,4,4,105,100,4,4,10,118,97,108,117,101,4,4,0,0]],"batch_id":1}
```

- 和 fetch_new 相同

## ack

通知 ape_dts 将 ack_batch_id 及之前的数据标为消费完成。

curl -X POST "http://127.0.0.1:10232/ack" -H "Content-Type: application/json" -d '{"ack_batch_id": 6}'

### 参数

- ack_batch_id：和 fetch_new 的参数 ack_batch_id 相同。

### 返回值

```
{"acked_batch_id":1}
```

- acked_batch_id：和 info 返回值 acked_batch_id 相同。

# 解析及消费

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)

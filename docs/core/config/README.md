# 配置

APIXIS 在导入配置模块时读取当前工作目录中的 `config.yaml`。没有文件时使用默认值；配置不会在运行中自动重新加载。

## 本地应用示例

```yaml
SERVER:
  base_dir: ./.apix/

LOG:
  debug_level: INFO
  show_event_dispatch: false
  buffer_size: 1024

PIPELINE:
  event_pipe_max_len: 65536
  event_loop_backpressure: 1024
```

## 目录与节点

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `SERVER.base_dir` | `./.apix/` | 数据根目录；APIXIS 在其下使用 `apixis` 子目录 |
| `SERVER.node_name` | `str(uuid.getnode())` | 节点显示名称，不是消息接收地址 |

远程节点的接收地址是 `pipe.mq_id`，不是 `node_name`。远程模式下该地址在进程加载配置时自动生成，重新启动进程会生成新的地址。

## 日志

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `LOG.debug_level` | `DEBUG` | 日志级别，如 `DEBUG`、`INFO`、`WARN`、`ERROR` |
| `LOG.trace` | `true` | 是否输出 trace 信息 |
| `LOG.show_event_dispatch` | `true` | 是否输出事件分发信息 |
| `LOG.buffer_size` | `1024` | 所有 logger 共享的待写日志条数上限，必须为正整数；满时淘汰最旧记录 |
| `LOG.max_log_file_size` | `10485760` | 单个日志文件的轮转大小阈值，单位为字节 |

文件写入的启停方式见[日志接口](../utils/README.md)。

## 队列与并发

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `PIPELINE.event_pipe_max_len` | `65536` | 默认本地事件队列和 mailbox 缓冲区容量，必须大于 0 |
| `PIPELINE.event_loop_backpressure` | `1024` | 并发前台事件分发数量上限；小于 128 时按 128 使用 |

队列满时异步发布会等待空位。并发限制针对前台事件处理；后台 handler 不受单独的并发上限保护，应由应用控制后台工作量。

## 远程网关

本地事件和图执行无需配置远程服务。启用远程模式时，需要可访问的配置/事件网关以及所选的消息代理。

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `REMOTE_GATEWAY.enable` | `false` | 是否启用远程模式，使用 YAML 布尔值 |
| `REMOTE_GATEWAY.base_url` | `http://localhost:28080` | 网关地址 |
| `REMOTE_GATEWAY.config_endpoint` | `/api/config` | 远程配置接口 |
| `REMOTE_GATEWAY.pipe_endpoint` | `/api/pipe` | 事件路由和节点列表接口 |
| `REMOTE_GATEWAY.max_retry` | `5` | 事件网关请求失败后的最大重试次数 |
| `REMOTE_GATEWAY.retry_initial_delay` | `1.0` | 事件网关重试初始间隔，单位秒；后续按倍数增长 |
| `REMOTE_GATEWAY.timeout` | `10.0` | 事件网关请求超时，单位秒 |

启用后会在加载配置时获取远程配置，读取失败会向外抛出异常。远程配置请求使用 10 秒超时；上表的重试和 timeout 参数用于事件网关请求，其中连接/请求错误及 HTTP 503 会重试。

本地配置覆盖远程配置，嵌套配置按字段合并。`EVENT_CHANNEL` 只读取节点本地配置，不从网关继承。

## 消息代理

`EVENT_CHANNEL.type` 可为 `kafka`（默认）或 `rabbitmq`，决定节点接收事件所用的 mailbox。发送事件仍通过 HTTP 网关。

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `EVENT_CHANNEL.kafka.bootstrap_servers` | `["localhost:9092"]` | Kafka broker 地址或地址列表 |
| `EVENT_CHANNEL.kafka.topic_prefix` | `apixis.mailbox` | mailbox topic 前缀 |
| `EVENT_CHANNEL.kafka.group_id_prefix` | `apixis.node` | 消费组前缀 |
| `EVENT_CHANNEL.rabbitmq.url` | `amqp://guest:guest@localhost/` | RabbitMQ 连接地址 |
| `EVENT_CHANNEL.rabbitmq.exchange` | `apixis.events` | direct exchange 名称 |
| `EVENT_CHANNEL.rabbitmq.queue_prefix` | `apixis.mailbox` | mailbox 队列前缀 |
| `EVENT_CHANNEL.rabbitmq.prefetch_count` | `100` | 预取消息数 |

例如，使用 RabbitMQ 接收远程事件：

```yaml
REMOTE_GATEWAY:
  enable: true
  base_url: http://localhost:28080

EVENT_CHANNEL:
  type: rabbitmq
  rabbitmq:
    url: amqp://guest:guest@localhost/
    exchange: apixis.events
    queue_prefix: apixis.mailbox
    prefetch_count: 100
```

Kafka topic、消费组和 RabbitMQ 队列的名称分别为对应前缀加 `.` 再加节点 `mq_id`。发送方使用网关返回的节点地址；获取节点列表与发送方式见[事件通道](../event/channels.md)。

[返回文档首页](../../README.md)

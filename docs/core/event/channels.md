# 事件通道、序列化与远程传输

`ApixEventPipe` 将事件通道分为三个固定角色：

| 通道 | 方向 | 默认实现 | 用途 |
| --- | --- | --- | --- |
| `builtin` | 读写 | `BuiltinChannel` | 当前进程内无限制的 ready 队列 |
| `mailbox` | 只读 | `KafkaChannel` 或 `RabbitMQChannel` | 接收网关投递给当前节点的远程事件 |
| `mailtruck` | 只写 | `GatewayChannel` | 通过 HTTP 网关发送或广播事件 |

`APIX_EVENT_LOOP` 只消费 `builtin`。远程 mailbox 收到的事件由 `ApixEventPipe` 的 forwarder 转发到 `builtin`，所以处理器无需区分事件来源。

## ApixEventPipe API

全局实例：

```python
from apixis.core.event import EVENT_PIPE
```

常用接口：

| 接口 | 说明 |
| --- | --- |
| `await post_event(...)` | 创建并发布一个事件 |
| `await put(event, channel=...)` | 发布已有事件 |
| `put_nowait(event)` | 无等待写入支持该操作的通道 |
| `await get(channel=...)` | 等待并取出事件 |
| `get_nowait(channel=...)` | 立即取出事件 |
| `empty()` / `full()` / `qsize()` | 查询通道本地缓冲区 |
| `task_done()` / `await join()` | asyncio.Queue 风格的完成跟踪 |
| `await clear()` | 移除并确认当前已经排队的事件 |
| `await send(event, recipient)` | 向指定远程节点发送事件 |
| `await broadcast(event)` | 广播节点级事件并更新节点目录 |
| `await start()` / `await stop()` | 打开或关闭外部通道与后台任务 |
| `nodes` | 返回远程节点目录的防御性副本 |
| `get_channel(name)` | 获取具体通道对象 |

## 本地事件

```python
await EVENT_PIPE.post_event(
    event_type=EventType.WORKFLOW,
    event_name="document.index.requested",
    context={"document_id": "doc-1"},
)
```

默认写入 `builtin`。全局管道的本地发布自动启动消费者，成功写入后记录精确事件名；发布端不查询或重建 handler_chain。

`builtin` 是无限制的 ready 队列，`maxsize == 0`，`full()` 始终为 `False`；本地 `put()` 不等待容量，`put_nowait()` 不会因为事件积压而抛出 `asyncio.QueueFull`。事件循环通过处理队列容量限制排队量，通过独立的分发额度限制执行量，详见[两阶段队列与背压](./README.md#两阶段队列与背压)。

`qsize()` 和 `empty()` 只描述 ready 中尚未接纳的事件；`clear()` 也只移除这些事件，不取消已接纳或正在分发的事件。`join()` 则直到所有本地事件被确认后才返回，包含已经离开 ready 的分发任务。

## 直接操作队列

一般由 `APIX_EVENT_LOOP` 消费 `builtin`，应用不应同时竞争消费同一全局队列。自定义管道或测试中可以使用队列接口：

```python
event = await EVENT_PIPE.get()
try:
    ...
finally:
    EVENT_PIPE.task_done()

await EVENT_PIPE.join()
```

每次成功 `get()` 后都必须有一次 `task_done()`，否则 `join()` 不会完成。

## 事件序列化

远程传输使用以下 wire payload：

```python
{
    "event_id": "event-...",
    "event_type": "workflow",
    "event_name": "agent.task.created",
    "context": {"task_id": "task-1"},
    "timestamp": 1787414400.0,
    "accepted": False,
    "error_stack": [],
}
```

`error_stack` 按顺序保存错误记录字典，每条包含 `handler_name`、`phase`、`exception_type`、`message` 和 traceback 文本。反序列化后恢复为 `ApixEventError`，因此下游仍可通过 `has_error` 判断前置错误。

前台 handler 的 `CancelledError` 记录也会保留。当前 wire payload 不包含 `seen`；从远程 payload 创建的新事件使用空列表，记录接收端后续的核心函数执行。

内部辅助函数位于 `apixis.core.event.event_pipe`：

```python
from apixis.core.event.event_pipe import (
    encode_event,
    event_from_json,
    event_to_json,
)
```

- `event_to_json(event)` 将 `ApixEvent` 转为字典。
- `encode_event(event)` 生成 UTF-8 JSON bytes，用于 Kafka 或 RabbitMQ。
- `event_from_json(payload)` 接受 mapping、JSON string、bytes，或直接返回传入的 `ApixEvent`，直接返回时保留其 `seen`。
- `event_from_json()` 也能解析 `{"event": {...}}` 形式的网关路由 envelope。

跨进程传输时，`context` 必须可 JSON 序列化。编码器额外支持 `Enum` 和 dataclass 实例；其他复杂运行时对象（例如 `GraphContext`、Future、文件句柄）只适合本地 `builtin` 通道。

## 远程发送

### 点对点发送

```python
event = ApixEvent(
    event_id="event-task-1",
    event_type=EventType.WORKFLOW,
    event_name="worker.execute",
    context={"task_id": "task-1"},
    timestamp=time.time(),
)

await EVENT_PIPE.send(event, recipient="worker-node-id")
```

`recipient` 必须是非空 mq id。`GatewayChannel` 通过 HTTP POST 请求网关的 pipe endpoint，并携带 sender、recipient 和事件 payload。

`GatewayChannel` 只提供异步写入、网关操作和生命周期方法，不提供同步写入、读取、`maxsize` 或队列状态接口。通过 `ApixEventPipe` 对 `mailtruck` 执行不支持的操作时，抛出 `EventChannelPermissionError`。

### 广播

```python
result = await EVENT_PIPE.broadcast(event)
nodes = EVENT_PIPE.nodes
```

当远程网关未启用时，`broadcast()` 直接返回空字典。

### 重试

网关请求对以下情况重试：

- `httpx.RequestError`；
- HTTP 503。

退避时间为 `retry_initial_delay * 2**retry`。其他非成功状态直接通过 `raise_for_status()` 抛出。

## mailbox

`mailbox` 是接收专用于当前节点的外部事件的只读通道：

- `KafkaChannel` 消费 `${topic_prefix}.${mq_id}`，group id 为 `${group_id_prefix}.${mq_id}`。
- `RabbitMQChannel` 声明 direct exchange，并用当前 `mq_id` 作为 routing key 绑定 `${queue_prefix}.${mq_id}` 队列。
- broker 消息被反序列化为 `ApixEvent`，进入本地缓冲区。
- forwarder 将其转发到 `builtin`；本节点分发器从处理队列出队时解析当前 handler_chain。

`KafkaChannel` 与 `RabbitMQChannel` 不提供写接口。通过 `ApixEventPipe` 向 mailbox 写入时，抛出 `EventChannelPermissionError`。

## 配置

`source/config.yaml` 中的相关配置如下：

```yaml
REMOTE_GATEWAY:
  enable: true
  base_url: "http://localhost:8080"
  config_endpoint: "/api/config"
  pipe_endpoint: "/api/pipe"
  max_retry: 5
  retry_initial_delay: 1.0
  timeout: 10.0

SERVER:
  node_name: "apix_service"

PIPELINE:
  event_pipe_max_len: 1024
  event_loop_backpressure: 1024
  background_handler_backpressure: 4096

EVENT_CHANNEL:
  type: "kafka"  # kafka | rabbitmq

  kafka:
    bootstrap_servers:
      - "localhost:9092"
    topic_prefix: "apixis.mailbox"
    group_id_prefix: "apixis.node"

  rabbitmq:
    url: "amqp://guest:guest@localhost/"
    exchange: "apixis.events"
    queue_prefix: "apixis.mailbox"
    prefetch_count: 100
```

Apixis 核心不管理数据库或共享缓存，远程事件模式无需配置 `DATA_STORE` 或 `CACHE`。

`EVENT_CHANNEL` 是节点本地配置，不会从远程配置中心继承，避免多个节点错误消费同一个 mailbox 身份。

## 自定义通道

可以为独立 `ApixEventPipe` 按通道角色注入自定义对象：

```python
pipe = ApixEventPipe(
    builtin=CustomBuiltinChannel(),
    mailbox=CustomMailboxChannel(),
    mailtruck=CustomGatewayChannel(),
    remote_enabled=True,
    mq_id="node-1",
    node_name="worker",
    channel_type="kafka",
)
```

注入 `builtin` 的通道必须实现无限制的 ready 缓冲并返回 `maxsize == 0`，否则构造时抛出 `ValueError`，防止重新引入发布回压死锁。`BuiltinChannel(maxsize=...)` 本身仍支持有界队列，可用于 mailbox 或独立的普通队列。

接口按能力拆分，均可从 `apixis.core.event` 导入：

| 接口 | 职责 | 对应通道 |
| --- | --- | --- |
| `BaseEventChannel` | `start()`、`close()` 生命周期 | 所有通道 |
| `ReadableEventChannel` | 生命周期、`get()`、`get_nowait()`、`maxsize`、`empty()`、`full()`、`qsize()`、`task_done()`、`join()` | mailbox |
| `WritableEventChannel` | 生命周期、异步 `put(event, **kwargs)` | mailtruck |
| `ReadWriteEventChannel` | 组合读写能力，增加 `put_nowait()` | builtin |

`start()` 默认无需操作；自定义通道实现 `close()` 以及对应能力的抽象方法即可。只读通道无需实现写方法，异步发送通道无需实现本地队列或同步写入方法。`BuiltinChannel` 也可作为 mailbox 注入。

`get_channel("builtin")`、`get_channel("mailbox")`、`get_channel("mailtruck")` 分别返回对应能力的类型，编辑器可以据此提示可用方法。直接使用通道对象时，仅调用其提供的方法；`ApixEventPipe` 继续负责校验通道角色并报告 `EventChannelPermissionError`。

迁移已有自定义通道时，将原来的 `BaseEventChannel` 父类替换为对应能力接口，并移除仅为满足旧抽象而抛出权限异常的方法。`BaseEventChannel` 现在只表达生命周期。

自定义 mailtruck 的 `put()` 应接收 `recipient` 关键字参数。启用远程模式时，还需提供异步 `broadcast(event)` 用于上下线广播；`fetch_nodes()` 为可选的节点发现能力。

远程模式关闭时，默认 mailbox 仍使用不可用占位通道，读取会抛出 `EventChannelUnavailableError`，表示传输未启用。

## 生命周期与失败处理

```python
await EVENT_PIPE.start()
try:
    await APIX_EVENT_LOOP.start()
    ...
finally:
    await APIX_EVENT_LOOP.stop()
    await EVENT_PIPE.stop()
```

`EVENT_PIPE.start()` 是幂等的。如果启动任一外部通道失败，已打开的通道会被关闭。

`EVENT_PIPE.stop()` 会：

1. 尝试广播 offline 生命周期事件；
2. 取消 mailbox forwarder；
3. 按 mailbox、mailtruck、builtin 顺序并发关闭通道；
4. 若存在错误，清理后重新抛出第一个错误。

缺少可选 broker 依赖时：

- Kafka 抛出 `EventChannelUnavailableError`，提示安装 `aiokafka`。
- RabbitMQ 抛出 `EventChannelUnavailableError`，提示安装 `aio-pika`。

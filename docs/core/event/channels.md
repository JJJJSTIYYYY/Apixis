# 事件通道

应用通常通过 `ApixEventPipe` 发布和接收事件。默认使用本地队列；跨节点通信需要启用[远程配置](../config/README.md)。

## 选择通道

| 通道名 | 方向 | 用途 |
| --- | --- | --- |
| `builtin` | 读写 | 本地事件队列，也是事件处理器的输入来源 |
| `mailbox` | 只读 | 从 Kafka 或 RabbitMQ 接收远程事件，并自动转入本地队列 |
| `mailtruck` | 只写 | 通过 HTTP 网关向其他节点发送事件 |

```python
from apixis import get_event_pipe

pipe = get_event_pipe()
builtin = pipe.get_channel("builtin")
```

向 `mailbox` 写入、从 `mailtruck` 读取，或对 `mailtruck` 使用同步写入接口，会抛出 `EventChannelPermissionError`。远程模式关闭时，读取 mailbox 会抛出 `EventChannelUnavailableError`。

## 本地发布与队列

```python
from apixis import EventType

await pipe.post_event(
    event_type=EventType.INFO,
    event_name="demo.event",
    context={"value": 1},
)
await pipe.put(event)
```

队列满时，异步发布会等待空位。`post_event()` 适合在 handler 内发布后续事件。

| 接口 | 行为 |
| --- | --- |
| `put_nowait(event)` | 立即写入；满时抛出 `asyncio.QueueFull` |
| `get()` / `get_nowait()` | 取出事件；后者在空队列上抛出 `asyncio.QueueEmpty` |
| `qsize()` / `empty()` / `full()` | 查看队列当前状态 |
| `task_done()` | 确认一个已取出的队列项目 |
| `join()` | 等待队列项目完成确认 |
| `clear()` | 丢弃并确认当前仍在队列中的项目，返回清理数量 |

上述方法默认使用 `builtin`，读取和队列检查接口也可以指定 `channel="mailbox"`。共享 event core 会自动消费 builtin；应用一般通过订阅处理事件，直接调用 `get()` 会与框架竞争同一队列。

`join()` 的含义取决于队列使用方式：共享 core 的 builtin 在事件分发后确认，mailbox 在事件转入 builtin 后确认。两者都不表示 handler 已执行完成；自行消费独立队列时，由调用方负责 `task_done()`。

## 跨节点发送

```python
await pipe.send(event, recipient="target-mq-id")
# Equivalent channel form:
await pipe.put(event, channel="mailtruck", recipient="target-mq-id")
```

`recipient` 必须是非空的目标节点 MQ 地址。它与节点显示名称不同。启动远程 pipe 后，可通过 `pipe.nodes` 查看已获知的节点信息，并用 `pipe.mq_id` 查看自身地址。`pipe.nodes` 返回本地已获取的信息副本，不会在每次访问时自动刷新远端列表。

也可以广播节点生命周期事件：

```python
await pipe.broadcast(event)
```

`broadcast()` 返回网关响应字典；关闭远程模式时返回空字典。节点的上线和离线广播由 pipe 生命周期自动管理。

远程发送要求 `ApixEvent`。载荷使用 JSON 表示，支持将 dataclass 转为字段映射、Enum 转为其值；接收后不会自动恢复原来的 Python 类型。其他对象需要先转换为可序列化数据。

## 启动、停止与恢复

共享组件优先通过 `await start_core()` 唤起。独立构造的 pipe 需要自行管理：

```python
await pipe.start()
print(pipe.is_running)
await pipe.stop()
```

- 重复启动已运行的 pipe 不会重复打开连接。
- `stop()` 关闭连接，但保留当前对象中已入队和待转发的事件；下次 `start()` 继续处理。保留范围是当前进程内的同一个 pipe 对象。
- 队列已满时也可以停止，无需先等待所有事件消费完成。
- 取消停止操作时，已开始的资源清理会先完成，再传播 `asyncio.CancelledError`；并发启停会等待当前生命周期操作结束。

远程接收或转发任务退出后，`pipe.is_running` 和 `EventCore.started` 为 `False`。再次调用 `pipe.start()`、`start_core()` 或 core getter 可发起恢复；没有定时自动重试。同步 getter 返回不代表恢复已完成。

上线广播在启动准备完成后发送。已尝试上线但启动失败或取消时，pipe 会补发离线广播并关闭资源。补偿失败会记录日志；网关不可达时，远端节点状态可能暂时未更新。

## 消息确认与重投

| Broker | 确认时机 | 停止时尚未进入本地缓冲区的消息 |
| --- | --- | --- |
| RabbitMQ | 进入 mailbox 缓冲区后确认 | 重新入队，等待再次投递 |
| Kafka | 进入 mailbox 缓冲区后提交该分区的下一条 offset | 不提交其位置，重启后可重新消费 |

无法解析的载荷会记录日志并丢弃，同时确认消息或推进其 offset。

Kafka 消费组没有有效已提交 offset 时，使用 `earliest` 从保留范围内最早的消息开始读取；有有效提交位置时从该位置继续。新消费组可能读取已有历史消息。每条消息等待一次 offset 提交，因此吞吐量受提交延迟影响。

Broker 确认代表本地接收成功，不代表业务处理完成。本地缓冲区不持久化，进程崩溃可能丢失已确认事件。入队后确认失败也可能产生重投；业务应使用 `event_id` 等实现幂等处理。

## 自定义通道

独立 pipe 可以通过 `builtin=`、`mailbox=`、`mailtruck=` 注入通道。builtin 必须是有界读写队列；mailbox 和 mailtruck 分别提供读取与写入能力。

| 基础类型 | 能力 |
| --- | --- |
| `BaseEventChannel` | `start()`、`close()`、`is_running` |
| `ReadableEventChannel` | 读取、队列状态和确认 |
| `WritableEventChannel` | 异步写入 |
| `ReadWriteEventChannel` | 读写及 `put_nowait()` |

内置实现为 `BuiltinChannel`、`GatewayChannel`、`KafkaChannel`、`RabbitMQChannel`。直接管理 broker channel 时使用 `start()` / `close()`；并发调用会串行执行。其 `is_running` 反映消费任务状态，退出后可再次启动恢复。

自定义 mailbox 若有后台任务，应重写 `is_running` 反映实际状态；基础实现默认返回 `True`。远程 pipe 使用的自定义 mailtruck 还需支持 `broadcast(event)`，可选提供 `fetch_nodes()`。

[事件 API](./README.md) · [配置](../config/README.md) · [文档首页](../../README.md)

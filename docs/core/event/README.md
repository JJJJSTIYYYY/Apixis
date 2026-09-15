# 事件系统

`apixis.core.event` 提供进程级异步事件运行时。它负责：

- 创建和发布 `ApixEvent`。
- 按大小写敏感的 glob 模式匹配处理器。
- 以优先级或显式相邻关系确定处理器顺序。
- 在事件出队时确定候选 handler 名称和顺序，调用前读取当前对象并重新检查订阅和过滤条件。
- 使用本地队列分发事件，并可通过网关、Kafka 或 RabbitMQ 在节点之间转发。
- 记录运行期间已经观察到的精确事件名，支持插件诊断。

实现按职责拆分：`factory.py` 管理组件构建与启动，`subscription.py` 提供注册便捷接口，
`event_pipe.py` 负责管道与转发，`pipe_channel.py` 定义通道能力、传输实现及序列化。

## 核心对象

| 对象 | 用途 |
| --- | --- |
| `ApixEvent` | 单个事件的数据模型 |
| `EventType` | 事件类别：`internal`、`workflow`、`lifecycle`、`info`、`warning`、`error` |
| `await start_core()` | 按依赖顺序构建并启动完整事件系统，重复调用复用对象 |
| `get_event_pipe()`/`aget_event_pipe()` | 默认全局事件管道 |
| `get_event_loop()`/`aget_event_loop()` | 默认全局事件消费者与分发器 |
| `subscribe()` | 注册异步事件处理器 |
| `unsubscribe()` | 立即移除处理器和排序桶记录 |
| `get_handler_registry()`/`aget_handler_registry()` | 处理器、排序桶和单份当前链缓存 |
| `get_event_registry()`/`aget_event_registry()` | 已观察到的精确事件名集合 |

**非必要建议使用getter的异步接口以达到更好的性能**

类型别名：

- `EventHandlerFunc` 表示 `Callable[[ApixEvent], Awaitable[None]]`。
- `ChannelType` 表示 `Literal["builtin", "mailbox", "mailtruck"]`。

## 发布和处理一个事件

```python
import asyncio

from apixis.core.event import (
    start_core,
    get_event_loop,
    get_event_pipe,
    ApixEvent,
    EventType,
    unsubscribe,
    subscribe,
)


@subscribe("agent.message.created", priority=10, exist_ok=False)
async def log_created_message(event: ApixEvent) -> None:
    print(event.event_name, event.context)


async def main() -> None:
    await start_core()
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO,
            event_name="agent.message.created",
            context={"message_id": "msg-1"},
        )
        await get_event_pipe().join()
    finally:
        unsubscribe(log_created_message.__name__)
        event_loop, event_pipe = get_event_loop(), get_event_pipe()
        await event_loop.stop()
        await event_pipe.stop()


asyncio.run(main())
```

`post_event()` 负责生成 `event_id` 和时间戳。若调用方已经构造了 `ApixEvent`，可改用 `await get_event_pipe().put(event)`。

## ApixEvent

```python
@dataclass(slots=True)
class ApixEvent:
    event_id: str
    event_type: EventType
    event_name: str
    context: Any
    timestamp: float
    accepted: bool = False
    seen: list[str] = field(default_factory=list)
    error_stack: list[ApixEventError] = field(default_factory=list)
```

公开成员：

| 成员 | 说明 |
| --- | --- |
| `event_id` | 事件唯一标识；`post_event()` 使用 `event-` 前缀生成 |
| `event_type` | `EventType` 枚举值 |
| `event_name` | 用于订阅匹配的精确名称，匹配时区分大小写 |
| `context` | 任意上下文；跨进程传输时必须可 JSON 序列化 |
| `timestamp` | Unix 时间戳 |
| `accepted` | 是否显式接受事件；后续 handler 跳过核心函数，仍执行通知 |
| `seen` | 按核心函数开始执行的顺序记录 handler 名称，每个事件持有独立列表 |
| `error_stack` | 按发生顺序保存前台 handler 的 `ApixEventError` 列表 |
| `has_error` | 只读属性，等价于 `bool(error_stack)` |
| `datetime` | 将 `timestamp` 转换为本地 `datetime` 的只读属性 |
| `accept()` | 将 `accepted` 设为 `True` |

事件不携带 handler_chain 版本。缓存与分发规则见[当前链缓存](./handlers.md#出队时解析当前链)。

`seen` 在每次 `core_func` 执行前追加当前 handler 的 `name`，因此核心函数内部已能看到自己的记录。
它表示开始执行，不代表成功完成；核心函数异常、超时或取消后，记录仍保留。
因 `accepted` 或 `stop_when_error` 跳过核心函数的 handler 不会被追加，仅执行通知回调也不会追加。
前台和后台 handler 都遵循这一规则；后台记录顺序取决于实际调度。重复执行同一事件会重复追加名称，
`seen` 不去重，也不自动阻止再次执行。远程序列化保留 `seen`；旧 payload 缺少该字段时，反序列化使用空列表。

## 接受事件与后续通知

前台处理器可以调用 `event.accept()`：

```python
@subscribe("request.*", priority=100)
async def reject_invalid_request(event: ApixEvent) -> None:
    if not event.context.get("authenticated"):
        event.context["error"] = "unauthorized"
        event.accept()
```

调用后，后续 handler 的 `core_func` 被跳过，但 `on_accepted` 仍会执行；如果已有错误，先调用 `on_has_error`。需要注意：

- 已经创建的后台处理器任务不会被撤销。
- 分发结束不会自动将事件标记为 accepted；该状态仅表示显式接受事件。
- 后台 handler 在实际开始执行时判断事件状态；尚未开始的任务也可能转入通知分支。
- `accept()` 控制的是当前事件实例，不会注销订阅。

## 事件循环

`ApixEventLoop` 从构造参数传入的 `event_pipe` 的 `builtin` ready 队列接纳事件，再交给内部处理队列分发。

### 两阶段队列与背压

1. 本地 `post_event()`、`put()`、`put_nowait()` 和 mailbox 转发均写入无限制的 ready 队列，发布不会等待分发额度。
2. 消费者从 ready 队列取出事件，通过 `await processing_queue.put(event)` 转入处理队列；队列满时等待容量，不占用分发额度。
3. 分发器先取得 `EVENT_LOOP_BACKPRESSURE` 额度，再从处理队列取出事件、记录非空精确事件名、确定候选 handler 名称及顺序，并创建分发任务。直到整个分发任务完成、异常或取消，才确认事件并释放额度。

当前实现将**处理队列容量和分发并发额度都设为 `max(128, EVENT_LOOP_BACKPRESSURE)`**，默认各为 1024。队列和信号量分别控制排队与执行，但使用同一个配置值；`EVENT_PIPE_MAX_LEN` 不控制处理队列。ready 中的事件和处理队列中的排队事件不占用正在执行的分发额度。消费者最多另外持有一个正在等待转入处理队列的事件；处理队列满时停止继续读取 ready。

`BACKGROUND_HANDLER_BACKPRESSURE` 继续独立限制后台 handler。分发任务等待后台额度时仍占用分发额度；后台任务成功创建后，其执行由后台额度独立跟踪。

`EVENT_PIPE_MAX_LEN` 用于默认外部 mailbox 的本地缓冲容量。ready 队列没有容量上限；持续超出处理速度的发布会增加 ready 积压和内存使用。

如需隔离测试或构建独立运行时，通过构造参数连接各组件：

```python
from apixis.core.event import (
    ApixEventRegistry, ApixEventPipe, ApixHandlerRegistry, ApixEventLoop,
)

event_registry = ApixEventRegistry()
event_pipe = ApixEventPipe()
handler_registry = ApixHandlerRegistry(event_registry)
event_loop = ApixEventLoop(handler_registry, event_pipe, event_registry)

await event_pipe.start()
await event_loop.start()
```

独立运行时只读取自己的管道和注册表；使用 `handler_registry.register_handler()` 注册独立处理器。
`subscribe()`、`unsubscribe()` 和 handler 的 `register()` / `unregister()` 便捷方法操作工厂管理的全局注册表。

### 启动

```python
from apixis.core.event import start_core

await start_core()
```

`factory.py` 按 `event_registry -> event_pipe -> handler_registry -> event_loop` 的顺序构建组件，
随后依次等待 `event_pipe.start()` 和 `event_loop.start()` 完成。重复调用只确保启动，不替换任何组件。
并发调用串行完成启动，避免重复打开远程连接或创建转发任务。启动失败或取消会传播给调用方，后续调用复用原组件重试。

四个 getter 是同步接口：首次访问时构建完整组件组。在运行中的 asyncio loop 内调用时，会调度一个共享启动任务；连续调用不会重复启动。
在 `asyncio.run()` 之前只构建组件，仍可同步注册处理器；进入 asyncio 后再次调用 getter 即可启动。
`NodeGraph` 通过 getter 获取管道时触发启动，不再显式调用 `start_core()`。本地发布本身只入队。停止后再次调用 getter 或 `await start_core()` 均可恢复消费。
同步 getter 返回时异步启动可能尚未完成；需要等待远程连接就绪或直接捕获启动异常时，使用 `await start_core()`。自动启动失败会记录日志，后续 getter 可重试。
各组件的构造函数与 `start()` 都只使用注入的依赖，不回调工厂 getter。
整个运行时应在同一个 asyncio loop 内使用。

### 停止

```python
await get_event_loop().stop()
```

`stop()` 停止 ready 消费者和处理队列分发器，不等待处理队列排空，也不等待正在执行的 handler。ready、处理队列及消费者正在等待转入的事件均保留；已经创建的分发任务和后台任务继续执行。重启后先继续转入被中断的事件，再读取后续 ready 事件，保持 FIFO。再次调用 getter 或 `await start_core()` 可恢复消费。它不负责关闭外部通道；完整关闭时应先保存组件引用，再依次停止，避免关闭过程中再次调用 getter 触发重启：

```python
event_loop, event_pipe = get_event_loop(), get_event_pipe()
await start_core()
await event_pipe.join()
await event_loop.stop()
await event_pipe.stop()
```

如需等待本地队列中的事件完成分发，应在停止消费之前执行：

```python
await get_event_pipe().join()
```

分发任务的完成回调统一确认队列项并释放分发额度；正常完成、异常和取消（包括任务尚未开始时取消）均只清理一次。后台任务在创建前取得并发额度，并由完成回调释放；等待额度期间取消不会创建后台任务。

`get_event_pipe().join()` 通过 ready 队列的完成计数覆盖 ready 等待、转入处理队列的等待、处理队列等待及正在分发的事件；转移到处理队列时不会提前确认。后台处理器由分发器独立调度，因此 `join()` 返回不等于所有后台处理器都已结束，也不保证包含它们未来才发布的事件。

## 分发错误策略

处理器异常和超时由 `ApixEventHandler.execute()` 统一处理，不会从 `get_event_pipe().post_event()` 反向抛给发布者，因为发布与处理是异步解耦的。

- `stop_when_error=True`：事件已有前置错误时，当前 handler 执行 `on_has_error`，跳过自己的 `core_func`。
- `stop_when_error=False`：执行错误通知后，事件未被 accepted 时继续运行自己的 `core_func`。
- `background=True`：核心函数和通知函数的未捕获普通异常、超时只记录日志，不写入 `error_stack`。回调显式修改共享事件或业务上下文仍会影响后续行为。
- `time_out=None`：无限等待；正数超时分别应用于每个实际调用的核心函数或通知函数。
- `time_out <= 0`：注册时被标准化为 `None`。

`on_has_error` 只负责处理前置 handler 的错误。当前 handler 的核心函数或通知函数抛出未捕获的普通异常时，先记录错误，再调用 `on_error(event, exception)`，不会回调自己的 `on_has_error`。`on_error` 自身再抛错仅记录，不递归调用；正常返回不会消除原始错误或重试失败函数。后台 handler 也调用 `on_error`，但异常不写入事件错误栈。

前台 handler 的 `core_func`、`on_has_error`、`on_accepted` 或 `on_error` 传播 `asyncio.CancelledError` 时，
先将取消记录追加到 `error_stack`，再重新抛出原始取消异常，不因此调用 `on_error`。
记录包含实际出错的 handler 和 phase，`has_error` 因而变为 `True`；正常 handler 链仍立即终止。
后台取消不写入事件错误栈。handler 的 `asyncio.timeout()` 正常转换出的超时仍记录为 `TimeoutError`。

需要局部恢复且不向事件记录错误时，仍在原函数内使用 `try/except/finally`。

`on_cancelled(event)` 负责取消收尾：正在执行的前台事件传播 `CancelledError` 后，事件循环跳过剩余业务函数，并发通知本次候选链中仍注册且匹配的前台 handler，等待全部通知完成后再重新抛出取消。独立后台任务取消仅通知它自身。取消回调沿用 handler 的超时设置；清理异常只写日志，不追加错误记录，也不改变原始取消结果。具体注册方式与触发范围见 [handlers.md](handlers.md#事件取消通知-on_cancelled)。

每条 `ApixEventError` 包含 `handler_name`、`phase`、`exception_type`、`message`、`traceback`。`phase` 为 `core_func`、`on_has_error`、`on_accepted` 或 `on_error`；traceback 保存文本，不保留异常对象或活动栈帧。序列化会保留这些字段。

如果业务需要确认处理结果，应通过事件上下文中的 Future、队列或其他显式回传机制实现，而不是依赖 `post_event()` 返回值。

## 观察到的事件名

`ApixEventLoop` 从背压处理队列取出事件后，统一调用注入的事件注册表的 `record_event(event)`，随后解析处理器链。只记录非空精确事件名，不保存事件对象；`EventType.INTERNAL` 被排除，但仍按普通订阅规则分发。

ready 队列和处理队列中尚未出队的事件不会被记录；仅发送到 mailtruck 或 broadcast 的事件也不计入本地观察记录。mailbox 转发的事件和直接写入 builtin 的事件只要进入本地分发，都会被统一记录，即使没有匹配的 handler。调用方仍可显式使用 `record_event(event)` 写入观察记录。

```python
from apixis.core.event import get_event_registry

observed = get_event_registry().get_registered_events()
print(observed)  # frozenset[str]
```

它的主要用途是诊断订阅是否覆盖了真实事件：

```python
from apixis.core.event import get_unmatched_subscriptions

unmatched = get_unmatched_subscriptions("my_plugin_handler")
```

`clear()` 只清空观察记录，不会清空队列、处理器或处理器链缓存：

```python
get_event_registry().clear()
```

`ApixEventRegistry` 是普通类，直接实例化会创建独立注册表。进程级复用由工厂负责，通过 `get_event_registry()` 取得共享实例；`ApixHandlerRegistry`、`ApixEventPipe` 和 `ApixEventLoop` 遵循相同的工厂所有权规则。

`record_event()` 对重复名称去重；非 `ApixEvent` 参数抛出 `TypeError`，非 INTERNAL 事件的空名称抛出 `ValueError`。内部读写使用 `RLock`，这不表示整个异步事件运行时可跨线程直接调用。

## 生命周期建议

应用启动时：

```python
await start_core()
```

应用退出时：

```python
event_loop, event_pipe = get_event_loop(), get_event_pipe()
await start_core()
await event_pipe.join()
await event_loop.stop()
await event_pipe.stop()
```

本地与远程模式共用 `start_core()` 启动入口。关闭后再次调用会启动原来的对象，并保留注册信息和待处理事件。

## 继续阅读

- [处理器注册、排序与当前链缓存](./handlers.md)
- [事件通道、序列化与远程传输](./channels.md)
- [Core Runtime 总览](../README.md)

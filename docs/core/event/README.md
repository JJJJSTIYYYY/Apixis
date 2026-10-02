# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 共享事件系统

### `await start_core()`

唤起共享 event core。已运行时重复调用不会重复启动；返回不表示后台消费者已开始处理事件，也不表示已发布事件处理完成。启动连接时发生的异常会传播给调用方。

共享 core 只构建一次，后续调用复用同一组 registry、pipe、handler registry 和 event loop。

本地消费者任务退出（包括被直接取消），或远程 mailbox 消费/转发任务退出后，core 不再报告已启动，后续 getter 或 `start_core()` 可在同一个 asyncio loop 内重新发起启动。此行为不承诺跨 asyncio loop 复用已有队列或连接。

```python
from apixis import start_core

await start_core()
```

### 同步 getters

- `get_event_registry()` → `ApixEventRegistry`
- `get_event_pipe()` → `ApixEventPipe`
- `get_handler_registry()` → `ApixHandlerRegistry`
- `get_event_loop()` → `ApixEventLoop`

同步 getter 会立即返回共享对象；处于运行中的 asyncio loop 时会触发启动，但**不保证启动完成**。

### 异步 getters

- `await aget_event_registry()`
- `await aget_event_pipe()`
- `await aget_handler_registry()`
- `await aget_event_loop()`

异步 getter 以 `await` 方式唤起并返回同一组共享组件，不等待已发布事件处理完成。

## `EventType`

```python
from apixis import EventType
```

可选值：

- `EventType.INTERNAL`
- `EventType.WORKFLOW`
- `EventType.LIFECYCLE`
- `EventType.INFO`
- `EventType.WARNING`
- `EventType.ERROR`

`INTERNAL` 事件不会被 `ApixEventRegistry` 记录为用户可观察事件名。它是 event core 自身的生命周期事件。

## `ApixEvent`

```text
ApixEvent(
    event_id: str,
    event_type: EventType,
    event_name: str,
    context: Any,
    timestamp: float,
    accepted: bool,
    seen: list[str],
    error_stack: list[ApixEventError]
)
```

`accepted` 默认为 `False`，`seen` 和 `error_stack` 各自默认为独立的空列表。

主要字段：

| 字段 | 说明 |
| --- | --- |
| `event_id` | 事件 ID |
| `event_type` | `EventType` |
| `event_name` | 用于 handler 匹配的事件名 |
| `context` | 用户上下文 |
| `timestamp` | Unix timestamp |
| `accepted` | 是否已被 accept |
| `seen` | 已进入 core callback 的 handler 名称 |
| `error_stack` | 前台 handler 产生的 `ApixEventError` 列表 |

### `event.accept()`

标记事件已被接受。后续 handler 不再执行自己的 core callback，但适用的错误通知和 accepted 通知仍可能执行。

### `event.has_error`

是否有已记录的前台 handler 错误。

### `event.datetime`

时间戳信息，会将 `timestamp` 转为本地 `datetime` 返回。

## 发布事件

推荐通过共享 `ApixEventPipe`：

```python
from apixis import EventType, get_event_pipe, start_core

await start_core()
pipe = get_event_pipe()

await pipe.post_event(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
)
await pipe.join()
```

`post_event()` 会创建 `ApixEvent` 并放入指定 channel；本地队列满时等待空位。

`pipe.join()` 等待队列项目完成交接，不等待 handler 执行结束。需要等待业务结果时，由处理器通过应用自己的结果对象或信号通知调用方，完整示例见[快速开始](../../quickstart.md)。

## `ApixEventPipe` 常用接口

```python
await pipe.put(event, channel="builtin", recipient=None)
await pipe.post_event(...)
pipe.put_nowait(event, channel="builtin")

event = await pipe.get(channel="builtin")
pipe.task_done(channel="builtin")
await pipe.join(channel="builtin")
await pipe.clear(channel="builtin")

await pipe.send(event, recipient)
await pipe.broadcast(event)
```

本地应用通常使用默认 `builtin` channel。远程 channel 见 [Event channels](./channels.md)。

## 订阅

```python
from apixis import subscribe


@subscribe("job.*")
async def observe_job(event):
    ...
```

支持 glob 风格模式。详细排序、错误语义与取消语义见 [Event handlers](./handlers.md)。

## 取消订阅与检查

```python
from apixis import (
    get_handler,
    get_handler_meta,
    get_unmatched_subscriptions,
    is_registered,
    unsubscribe,
)

unsubscribe("observe_job")
```

- `unsubscribe(name, missing_ok=True)`：移除 handler；
- `get_handler(name)`：返回 handler 或 `None`；
- `get_handler_meta(name)`：读取注册元数据；
- `is_registered(name)`：检查是否注册；
- `get_unmatched_subscriptions(name)`：返回当前尚未观察到匹配事件名的订阅模式。

## 已观察到的事件名

`ApixEventRegistry` 可查询本地已经观察到的精确事件名；它不保存事件内容，也不能用于查询某个事件是否处理完成。

```python
from apixis import get_event_registry

registry = get_event_registry()
seen = registry.get_registered_events()  # frozenset[str]
registry.clear()
```

## 在 handler 内发布和等待

在 handler 内发布后续事件，优先使用 `pipe.post_event()`。它会在等待发布期间让出当前事件的调度容量。

若 handler 等待另一个事件的处理结果，在并发容量耗尽时可能相互阻塞。优先将后续逻辑放到结果事件的订阅处理器中，避免把整条业务流程串成相互等待的 handler。图中的嵌套 `invoke()` 和 `interrupt()` 已提供相应的等待行为，见[图 API](../graph/README.md)。

## 暂停消费与关闭通道

应用入口可以显式管理共享组件：

```python
from apixis import get_event_loop, get_event_pipe

event_loop = get_event_loop()
pipe = get_event_pipe()
await event_loop.stop()
await pipe.stop()
```

`event_loop.stop()` 暂停取出新事件，保留队列和待分发事件；已经启动的前台处理和后台任务继续运行，停止接口不会等待它们完成。关闭应用前，应先等待应用自身需要完成的业务工作。`pipe.stop()` 的远程连接和消息保留行为见[事件通道](./channels.md)。

[处理器](./handlers.md) · [事件通道](./channels.md) · [文档首页](../../README.md)

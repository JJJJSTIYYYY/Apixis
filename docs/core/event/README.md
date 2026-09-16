# Event API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## Core lifecycle

### `await start_core()`

启动 event core 后台的消费者任务，该方法只发出启动命令，不保证返回时启动已完成。若重复调用此接口，接口检测到启动命令已成功发出后立即返回。

共享 core 只构建一次，后续调用复用同一组 registry、pipe、handler registry 和 event loop。

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

异步 getter，效果和同步接口维持一致。

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

```python
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

`post_event()` 会创建 `ApixEvent` 并放入指定 channel。

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

## Event registry

`ApixEventRegistry` 只记录本地运行时实际观察到的精确事件名，不持有事件对象，也不参与 dispatch。

```python
registry = get_event_registry()
seen = registry.get_registered_events()  # frozenset[str]
registry.clear()
```

## 死锁风险

若一个事件 handler 依赖后续事件的上下文或处理结果，不推荐使用在前置事件的 handler 中等待一个 future，在后续事件的 handler 中 set_result，在并发量达到背压阈值时，容易由于前置事件 handler 被挂起、后续事件无法被处理而出现死锁。
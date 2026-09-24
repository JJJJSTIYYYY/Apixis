# Event handlers

## `@subscribe(...)`

```python
subscribe(
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
)
```

最常见用法：

```python
from apixis import subscribe


@subscribe("order.*")
async def audit(event):
    ...
```

### 事件名匹配

`event_names` 支持 glob 模式，例如：

```python
@subscribe("order.*", "payment.?")
async def observe(event):
    ...
```

`filter_event` 用于排除匹配项。

### 替换语义

默认 `exist_ok=True`。如果同名 handler 已注册，新注册会替换旧注册。

设置 `exist_ok=False` 时，同名注册会抛出 `EventHandlerAlreadyRegisteredError`。

消费者取出事件时，会按当时的订阅、过滤条件和顺序生成 handler **引用列表**，随后才等待调度容量。此事件的前台执行、后台任务和取消通知都使用这份列表，不再查询 registry 或重新匹配。后续新增、注销、替换注册或调整排序，只影响之后取出的事件；暂停并重启消费者也不会重新解析已取出事件的列表。

例如，一个事件已取出并捕获了后台 handler A，随后同名注册被前台 handler B 替换：这个事件仍调用 A；之后取出的事件使用 B，并按前台方式执行。

列表保存的是实例引用，不复制 handler 本身。直接修改已捕获实例的 callback 或执行选项（包括重新注册同一个实例时修改这些选项）仍会被观察到。

## 排序

### `priority`

数值越大，排序越靠前。

```python
@subscribe("order.*", priority=100)
async def validate(event):
    ...
```

### `between_handlers`

用于将 handler 放到两个已注册 handler 之间，不能与显式 `priority` 同时使用。

```python
@subscribe(
    "order.*",
    between_handlers=("validate", "persist"),
)
async def enrich(event):
    ...
```

边界的一侧可以为 `None`，但不能两侧同时为 `None`。`between_handlers` 具体插入规则如下：

- (left, right)：插入 left 与 right 之间，若其间已有 handler，则插入已由 handler 之后（即 right 前）
- (left, None)：插入 left 之后
- (None, right)：插入 right 之前
- (None, None)：抛出 ValueError

## `ApixEventHandler`

需要自定义生命周期回调时，可以显式创建 handler：

```python
from apixis import ApixEventHandler

handler = ApixEventHandler(
    core_func,
    on_accepted=on_accepted,
    on_has_error=on_has_error,
    on_error=on_error,
    on_cancelled=on_cancelled,
    stop_when_error=True,
    time_out=None,
    background=False,
    name="worker",
)
handler.register("work.*")
```

若参数 name 未指定，将默认使用 core_func 的名字作为 handler 名，后续动态 set_core_func 不会因为 core_func 改变而更新。

### 回调语义

| 回调 | 触发条件 |
| --- | --- |
| `core_func(event)` | handler 正常执行 |
| `on_has_error(event)` | 前置 handler 已产生错误 |
| `on_accepted(event)` | 事件已被 accept |
| `on_error(event, exc)` | 当前 handler 自身回调抛出普通异常 |
| `on_cancelled(event)` | event loop 通知取消 |

`on_has_error` 是**前序 handler 错误通知**，不是 `core_func` 自身异常处理的替代品。

### `stop_when_error`

当事件已有错误时：

- `True`：在 `on_has_error` 之后跳过当前 core callback；
- `False`：仍继续执行当前 core callback。

### `time_out`

正数表示每个被调用 callback 的超时时间。`None` 或非正值表示不限制。

### `background`

后台 handler，与前台 dispatch 解耦。后台 handler 的错误仍会记录日志并触发自身 `on_error`，但不会写入事件 `error_stack`，也不会阻断其他 handler。

## Accept

任意 handler 可调用：

```python
event.accept()
```

之后的 handler：

1. 如适用，执行 `on_has_error`；
2. 执行 `on_accepted`；
3. 不再执行 core callback。

## 错误记录

前台 handler 的未捕获异常会转成 `ApixEventError`，追加到：

```python
event.error_stack
```

其中记录：handler 名、阶段、异常类型、message 和 traceback 文本。

`asyncio.CancelledError` 也会被前台 handler 记录到 `error_stack`，随后继续向上传播。

## 动态更新 callback

```python
handler.set_core_func(new_core_func)
handler.add_has_error_callback(callback)
handler.add_on_accepted_callback(callback)
handler.add_on_error_callback(callback)
handler.add_on_cancelled_callback(callback)
```

`set_core_func()` 不改变 handler 身份和当前注册元数据。

## 注册与注销

```python
handler.register("event.*", priority=10)
handler.unregister()
```

这些实例方法与 `subscribe()` / `unsubscribe()` 使用同一套 registry 语义。

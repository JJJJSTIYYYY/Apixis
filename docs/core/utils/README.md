# Core 异常与辅助工具

`apixis.core.utils.exception` 定义事件系统和 Graph Runtime 的公共异常，全部由 `apixis.core.utils` 重新导出。`apixis.core.event` 也导出其中大部分异常，但不包含 `BlockHookNotRegisteredError`；后者应从 utils、`apixis.core` 或顶层 `apixis` 导入。

## 事件异常

### EventHandlerNotRegisteredError

处理器名称不存在，或 `between_handlers` 引用的边界处理器已经不再 active 时抛出。

常见来源：

- `unsubscribe(..., missing_ok=False)`；
- `get_unmatched_subscriptions()` 使用未知名称；
- 注册时指定不存在的 `between_handlers` 边界。

### EventHandlerAlreadyRegisteredError

处理器函数名已经存在且 `exist_ok=False` 时抛出。处理器名称在进程全局 registry 中唯一。

### EventChannelError

事件通道错误的 RuntimeError 基类。

### EventChannelPermissionError

同时继承 `PermissionError` 和 `EventChannelError`，表示通道方向不支持当前操作，例如：

- 向只读 mailbox 写入；
- 从只写 mailtruck 读取；
- 对 mailtruck 使用 `put_nowait()`。

### EventChannelUnavailableError

配置的通道当前不可用，例如远程网关未启用却读取 mailbox，或缺少 Kafka/RabbitMQ 客户端依赖。

## 图异常

### InvalidNodeReturnsError

普通节点返回了运行时无法转换成 `Command` 的值，例如 `None`、整数或 `list[Command]`；也可能是 `Command.update` 不是 mapping，或 `goto` 类型非法。

### GraphNodeError

图的前置前台事件处理器发生普通异常或超时时，内置 dispatch handler 的 `on_has_error` 会使用 `GraphNodeError` 结束调用；`errors` 保存此前的事件错误记录。中断事件的前置错误也通过 `block.fail(GraphNodeError(...))` 传给等待中的节点。

节点自身错误通常直接传播原异常，如 `InvalidNodeReturnsError`、`TimeoutError` 或 `ValueError`，不统一包装为 `GraphNodeError`。

### InvalidContextError

`invoke()` / `stream()` 收到其他图的 context、非 pending context，或未由当前图管理的 context 时抛出。`apply_command()` 的 context 归属或管理校验失败也使用该异常。

并非所有 context 错误都使用此类型：`graph.abort()` 对不属于当前图管理的 pending/running context 抛出 `ValueError`；`restore_context()` 对外图快照或 context 也抛出 `ValueError`；参数类型错误通常为 `TypeError`。

### BlockHookNotRegisteredError

默认中断处理器执行时，若本次事件的 `seen` 只有它自己，表示此前没有其他核心函数执行，将此异常传给等待的节点。检查依据是执行记录，不是注册表中是否存在 `interrupted_hook`。节点可以捕获此异常；未捕获时图调用失败。详见[图中断](../graph/interrupter/README.md)。

## 导入示例

```python
from apixis.core.event import (
    EventChannelError,
    EventChannelPermissionError,
    EventChannelUnavailableError,
    EventHandlerAlreadyRegisteredError,
    EventHandlerNotRegisteredError,
    InvalidNodeReturnsError,
)
```

或从定义模块导入：

```python
from apixis.core.utils.exception import (
    BlockHookNotRegisteredError,
    GraphNodeError,
    InvalidContextError,
)
```

## 错误处理边界

普通事件处理器的异常由 `ApixEventHandler` 记录并触发相应通知，发布与处理异步解耦，因此不会反向传播给事件发布者。前台取消记入错误栈后继续传播，取消清理由事件循环发起。Graph 节点异常通过 `GraphContext.completion` 传播给 `NodeGraph.invoke()` 或 `NodeGraph.stream()` 的调用方。

```python
try:
    result = await graph.invoke(state)
except InvalidNodeReturnsError:
    ...
except TimeoutError:
    ...
```

对于事件订阅生命周期错误，应在插件安装/卸载阶段显式检查，而不是依赖分发日志：

```python
try:
    unsubscribe(
        "required_handler",
        missing_ok=False,
    )
except EventHandlerNotRegisteredError:
    ...
```

## 日志与生命周期

`Logger`、`logger`、`auto_init` 和 `resource_cleaner` 也由 `apixis.core.utils` 导出。

| 接口 | 当前行为 |
| --- | --- |
| `logger.info/debug/warning/error/success(...)` | 按日志级别筛选，输出终端并缓存 |
| `logger.exception(...)` | 当前与 error 一样记录传入文本，不自动附加 traceback |
| `Logger.start()` / `Logger.stop()` | 启动刷盘任务；停止时刷出剩余缓存 |
| `Logger.flush()` | 主动写出当前日志缓存 |
| `auto_init.register(service)` | 注册有 `start`、`stop` 属性的服务，重复对象忽略 |
| `auto_init.start()` / `stop()` | 按注册顺序启动、逆序停止；单服务普通异常记录后继续 |
| `resource_cleaner.register(func)` | 登记不带参数的清理函数；调度支持普通返回值及 coroutine |
| `resource_cleaner.auto_clear(func)` | 注册原函数并返回异步 wrapper；直接调用 wrapper 时会 await 原函数结果 |
| `resource_cleaner.start()` / `stop()` | 启动或停止周期清理任务，无 interval 参数 |
| `resource_cleaner.run_once()` | 按登记顺序执行一轮；普通异常隔离，int 返回值用于累计清理数量 |

导入时 `resource_cleaner` 会注册到 `auto_init`，但不自动启动。`Logger`、`EVENT_PIPE` 和 `APIX_EVENT_LOOP` 未自动注册到 `auto_init`；需要由应用显式管理。清理间隔见[配置](../config/README.md)。

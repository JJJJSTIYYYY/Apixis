# 异常与日志

## 框架异常

以下异常可从 `apixis` 导入。应用应按异常类型处理失败，避免依赖具体错误文本。

| 异常 | 含义 |
| --- | --- |
| `BlockHookNotRegisteredError` | 图发生中断，但没有可执行的中断处理器 |
| `BlockNotResolvedError` | 中断处理器返回时，Block 既未完成也未被接管 |
| `EventHandlerAlreadyRegisteredError` | 禁止覆盖时注册了同名处理器 |
| `EventHandlerNotRegisteredError` | 所需处理器尚未注册 |
| `GraphNodeError` | 图节点执行错误 |
| `InvalidContextError` | 图执行上下文不合法 |
| `InvalidNodeReturnsError` | 节点返回值不符合要求 |

参数校验还可能抛出 `ValueError` 或 `TypeError`。异步取消使用 `asyncio.CancelledError`，不属于普通 `Exception`。

## 输出日志

```python
from apixis import Logger, logger

logger.info("Application started")
logger.warning("A retry is needed")
logger.error("Operation failed")
logger.debug("Preparing request")

worker_logger = Logger(name="worker")
worker_logger.info("Job received")
```

`logger` 是共享日志对象。需要按组件区分日志时，可创建命名的 `Logger`。在异常处理分支中可调用 `logger.exception("Operation failed")` 输出异常信息。

## 保存日志文件

由应用入口管理日志刷新：

```python
await Logger.start()
try:
    await run_application()
finally:
    await Logger.stop()
```

- `Logger.start()` 启动后台刷新。
- `Logger.flush()` 立即写出当前保留的记录。
- `Logger.stop()` 停止已启动的刷新任务，并写出剩余记录。

日志按 logger 名称写入 `SERVER.base_dir` 下的 `apixis` 目录。所有 logger 共享待写缓存，只保留最近 `LOG.buffer_size` 条记录，默认 1024 条；满时淘汰最旧记录，不影响控制台输出。

后台刷新由缓存达到条数或大小阈值触发，不保证低流量日志定时落盘。需要立即保存时调用 `flush()`；已被淘汰的记录无法再写入文件。未启动后台刷新时，也可以直接调用 `flush()`。

[日志配置](../config/README.md) · [文档首页](../../README.md)

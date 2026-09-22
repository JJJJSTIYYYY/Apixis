# Utilities API

## Exceptions

以下异常从 `apixis` / `apixis.core` 公共导出：

- `BlockHookNotRegisteredError`
- `EventChannelError`
- `EventChannelPermissionError`
- `EventChannelUnavailableError`
- `EventHandlerAlreadyRegisteredError`
- `EventHandlerNotRegisteredError`
- `GraphNodeError`
- `InvalidContextError`
- `InvalidNodeReturnsError`

调用方应优先捕获这些语义异常，而不是依赖内部错误文本。

## Logger

```python
from apixis import Logger, logger
```

`logger` 是共享日志对象；`Logger` 为其类型。

所有 logger 共享一个 FIFO 内存缓存，只保留最近 `LOG.buffer_size` 条日志，默认 1024 条（配置常量 `LOG_BUFFER_SIZE`）。缓存满时淘汰最旧记录；此限制在未启动日志刷新任务时也生效，不影响控制台输出。

`await Logger.start()` 启动后台刷新；`await Logger.flush()` 将当前保留的记录按 logger 名称写入各自文件并清空缓存；`await Logger.stop()` 停止已启动的刷新任务并写出剩余记录。已被 FIFO 淘汰的记录不会再写入文件。

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
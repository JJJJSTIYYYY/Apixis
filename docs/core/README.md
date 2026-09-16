# Core API

`apixis.core` 汇总导出 Event、Graph 和 Utils 的公共接口；顶层 `apixis` 再次导出同一组接口，并额外提供 `VERSION`。

推荐应用代码直接使用：

```python
from apixis import GraphManager, subscribe
```

而不是依赖内部文件路径。

## Event

事件系统负责：

- 发布与消费 `ApixEvent`；
- 按事件名或 glob 通配符匹配 handler；
- handler 排序、accept、错误与取消通知；
- 本地队列与可选远程通道；
- 全局共享 event core 生命周期。

详见 [Event API](./event/README.md)。

## Graph

图系统负责：

- 使用 `GraphManager` 构建并编译 `NodeGraph`；
- 通过 `Command` 更新 state 和选择下一跳；
- 并行节点/并行下一跳；
- `GraphContext`、快照恢复、stream；
- `interrupt()` / `Block` 中断交互。

详见 [Graph API](./graph/README.md)。

## API 稳定边界

公共 API 以各模块 `__all__` 为准。测试、诊断或框架开发中虽然可以访问内部属性，但 PyPI 使用方不应把 `_started`、内部 queue、registry 字典等实现细节作为兼容契约。

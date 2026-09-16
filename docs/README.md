# APIXIS 接口文档

本文档面向通过 PyPI 安装 APIXIS 的调用方，接口以当前 `apixis` / `apixis.core` 公共导出为准。除非调试框架本身，不建议依赖以下划线开头的属性或模块内部对象。

## 安装与运行环境

```bash
pip install apixis
```

- Python：`>= 3.12`
- 异步运行时：`asyncio`
- 推荐直接从 `apixis` 导入稳定公共接口。

## API 导航

| 模块 | 用途 |
| --- | --- |
| [Event API](./core/event/README.md) | 事件发布、订阅、handler、event core 生命周期 |
| [Event handlers](./core/event/handlers.md) | 排序、通配符、错误/accept/cancel 回调 |
| [Event channels](./core/event/channels.md) | builtin、gateway、Kafka、RabbitMQ 通道 |
| [Graph API](./core/graph/README.md) | 构图、执行、stream、context、interrupt |
| [Graph state & routing](./core/graph/state.md) | `Command`、`AutoMerge`、`KeepRef`、`Reset`、并行路由 |
| [Utilities](./core/utils/README.md) | 公共异常、logger、生命周期工具 |
| [Configuration](./core/config/README.md) | 运行时配置项及默认值 |

## 最小图调用

```python
from apixis import END, START, GraphManager


def step(state: dict) -> dict:
    return {"count": state.get("count", 0) + 1}


graph = (
    GraphManager()
    .add_node(step)
    .add_edge(START, "step")
    .add_edge("step", END)
    .compile_graph()
)

result = await graph.invoke({"count": 0})
graph.decompose()
```

## 最小事件调用

```python
from apixis import EventType, get_event_pipe, start_core, subscribe


@subscribe("app.*")
async def observe(event):
    print(event.event_name, event.context)


await start_core()
await get_event_pipe().post_event(
    event_type=EventType.INFO,
    event_name="app.started",
    context={"ready": True},
)
await get_event_pipe().join()
```

`get_*()` 与 `aget_*()`：它们会返回共享组件，并在已有 event loop 时触发后台启动，但不保证启动已经完成。其获取到的组件接口可用性均不依赖是否启动完成。

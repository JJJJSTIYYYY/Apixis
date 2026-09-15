# APIXIS Core Runtime

`apixis.core` 是 APIXIS 的核心运行时，包含两套相互协作但职责清晰的基础设施：

- `apixis.core.event`：异步事件发布、通配符订阅、确定性排序、出队解析当前处理器链，以及本地或远程事件通道。
- `apixis.core.graph`：基于事件系统驱动的状态图，包括节点、路由、状态合并、流式输出、运行上下文、快照恢复与人在环中断。

`apixis.core.config` 负责配置加载；`apixis.core.utils` 提供异常、日志、生命周期服务和 ID 生成工具。

## 模块结构

以下路径均相对于 `source/apixis/core/`：

| 路径 | 职责 |
| --- | --- |
| `config/base.py`、`config/core_config.py` | YAML/远程配置加载与运行时常量 |
| `event/base.py` | 事件、错误记录和处理器 |
| `event/factory.py`、`event/subscription.py` | 组件构建、启动和全局订阅便捷接口 |
| `event/event_loop.py`、`event/event_pipe.py` | 本地调度、背压和管道编排 |
| `event/pipe_channel.py` | 通道能力、外部传输和序列化 |
| `event/event_registry.py`、`event/handler_registry.py` | 已观察事件名、订阅、排序与当前链缓存 |
| `graph/base.py` | marker、Command、常量与图注册表 |
| `graph/graph_manager.py`、`graph/node.py`、`graph/node_graph.py` | 图构建、节点和编译图执行 |
| `graph/utils/namespace.py`、`state.py`、`validate.py` | 命名空间、状态复制/解析、图定义校验 |
| `graph/context/graph_context.py`、`manager.py`、`stream_writer.py` | 调用状态、ContextVar 与流输出 |
| `graph/interrupter/block.py`、`graph_interrupter.py` | Block 与中断流程 |
| `utils/exception.py`、`logger.py` | 异常与日志 |
| `utils/lifespan/` | 服务启动/停止与周期清理 |
| `utils/id_generator.py`、`utils/snow/` | ID 生成器实例与 Snowflake 实现 |

## 运行模型

Graph Runtime 并不在 `NodeGraph` 对象中保存每次调用的状态。一次调用的状态由 `GraphContext` 持有，并通过统一的图调度事件驱动执行：

1. `GraphManager` 编译图时，为当前 namespace 注册一个通用 `GRAPH_DISPATCH` handler。
2. `NodeGraph.invoke()` 或 `NodeGraph.stream()` 创建 context，或接受该图已准备的 context，并在检查归属与生命周期后绑定本次运行。
3. 新 context 从 `START` 开始，同图恢复 context 使用快照目标；运行时向 `get_event_pipe()` 发布 namespace 隔离后的 `GRAPH_DISPATCH` 事件。
4. 全局 `get_event_loop()` 消费事件，通用 dispatch handler 根据 `target_node_name` 执行单个目标节点或有序并发批次。
5. 节点返回 `dict` 或 `Command`；批次结果按目标顺序收集和提交，再把一个或多个下一目标写回 `target_node_name` 并发布同一个 dispatch 事件。
6. 当目标变为 `END` 时，dispatch handler 将最终状态写入完成 Future，调用方得到结果。

这套模型具有以下性质：

- 同一个编译图可以并发执行多次，因为状态和节点路由目标都由调用级 `GraphContext` 隔离。
- 一次调用内部也可通过 `Command(goto=[...])` 并发调度多个图节点；每个节点使用独立 state 副本，但共享该调用的只读 context。
- 事件系统只负责图级 dispatch 和 namespace 隔离，不再用具体节点名承担内部路由职责；插件需要观察节点调度时，可订阅 `graph.dispatch_name`；只有需要筛选特定节点时，才检查 `event.context.target_node_name`。图的处理器名也为 `graph.dispatch_name`，可直接用于 `between_handlers`。

## 最小示例

以下示例创建 `prepare -> answer -> END` 图。事件循环会在第一次调用时自动启动。

```python
import asyncio
from typing import Annotated, TypedDict

from apixis.core.graph import AutoMerge, END, GraphManager, START


class AgentState(TypedDict, total=False):
    prompt: str
    history: Annotated[list[str], AutoMerge()]
    answer: str


def prepare(state: AgentState) -> dict:
    return {"history": [f"user:{state['prompt']}"]}


async def answer(state: AgentState) -> dict:
    await asyncio.sleep(0)
    return {
        "answer": state["prompt"].upper(),
        "history": ["assistant:done"],
    }


async def main() -> None:
    graph = (
        GraphManager(AgentState)
        .add_nodes([prepare, answer])
        .add_edge(START, "prepare")
        .add_edge("prepare", "answer")
        .add_edge("answer", END)
        .compile_graph(using_namespace="quickstart")
    )

    try:
        result = await graph.invoke(
            {"prompt": "hello", "history": []}
        )
        print(result)
    finally:
        graph.decompose()


asyncio.run(main())
```

预期结果中的 `history` 会被 `AutoMerge` 合并：

```python
{
    "prompt": "hello",
    "history": ["user:hello", "assistant:done"],
    "answer": "HELLO",
}
```

## 常用导入

### 事件系统

```python
from apixis.core.event import (
    start_core,
    get_event_loop,
    get_event_registry,
    get_handler_registry,
    get_event_pipe,
    ApixEvent,
    EventType,
    get_handler_meta,
    get_unmatched_subscriptions,
    subscribe,
    unsubscribe,
)
```

### 图运行时

```python
from apixis.core.graph import (
    AutoMerge,
    BaseNode,
    Command,
    END,
    GraphManager,
    KeepRef,
    Node,
    NodeGraph,
    ParallelNode,
    Reset,
    START,
)

from apixis.core.graph.context import (
    GraphContext,
    get_current_namespace,
    get_current_run_id,
    get_graph_context,
    get_stream_writer,
)

from apixis.core.graph.interrupter import (
    Block,
    interrupt,
    interrupted_hook,
)
```

## 生命周期管理

### 全局事件运行时

`NodeGraph` 通过工厂 getter 获取管道时触发核心启动，图内无需显式调用 `start_core()`，图调用前通常也不需要手动启动。应用关闭或测试收尾时应主动停止事件循环：

```python
from apixis.core.event import get_event_loop, get_event_pipe, start_core


async def shutdown_core_runtime() -> None:
    event_loop, event_pipe = get_event_loop(), get_event_pipe()
    await start_core()
    await event_pipe.join()
    await event_loop.stop()
    await event_pipe.stop()
```

四个同步 getter 在 asyncio 中调度共享启动任务：先启动管道（包括启用的远程通道及转发任务），再启动事件循环。需要等待启动完成或捕获启动异常时，在异步入口执行 `await start_core()`。停止期间应使用事先保存的组件引用，避免再次调用 getter 触发重启。

### 编译图

每个已编译图会占用一个 dispatch listener 命名空间。使用完成后调用 `graph.decompose()`，或使用同步上下文管理器：

```python
with (
    GraphManager()
    .add_node(lambda state: {"done": True}, "work")
    .add_edge(START, "work")
    .compile_graph(using_namespace="temporary")
) as graph:
    result = await graph.invoke({})
```

默认分解会中止图管理的未结束 context；传入 force=False 时才拒绝带有未结束 context 的分解。`decompose()` 成功后会注销图拥有的通用 dispatch handler 和中断钩子，并释放命名空间；该 `NodeGraph` 不能再次调用。

## 并发边界

- 不同事件实例由独立分发任务处理；处理队列容量与分发任务数上限都使用 `max(128, EVENT_LOOP_BACKPRESSURE)`，默认各为 1024。
- 后台事件处理器另由 `BACKGROUND_HANDLER_BACKPRESSURE` 限制，默认 4096。
- 同一个 `NodeGraph` 可并发调用；普通字段会按深拷贝隔离。
- 一次调用的图级并发批次按路由列表顺序收集 Command；`AutoMerge` 字段确定性合并，普通字段的跨节点重复更新会使批次失败。
- `ParallelNode` 是单个图节点内部的并发分支；它可以作为一个成员加入图级并发批次，其内部分支共享该节点自己的 state 副本。
- `KeepRef` 会刻意破坏字段级深拷贝隔离，因此不推荐在并发调用中共享可变对象。
- `ContextVar` 绑定在节点执行期间有效。由节点创建的 asyncio task 会继承当前上下文；若任务生命周期超过节点，应避免继续使用已经结束或关闭的运行资源。
- 编译图的命名空间必须唯一；`exist_ok=True` 会强制分解旧图，包括中止旧图尚未结束的 context，然后接管命名空间。

## 文档导航

- [事件系统](./event/README.md)
- [处理器注册、排序与当前链缓存](./event/handlers.md)
- [事件通道、序列化与远程传输](./event/channels.md)
- [Graph Runtime](./graph/README.md)
- [状态模型、Command 与复制语义](./graph/state.md)
- [GraphContext、快照、恢复与流式上下文](./graph/context/README.md)
- [图中断与恢复控制](./graph/interrupter/README.md)
- [配置](./config/README.md)
- [Core 异常与辅助工具](./utils/README.md)

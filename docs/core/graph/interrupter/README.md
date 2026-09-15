# 图中断与恢复控制

`apixis.core.graph.interrupter` 用于在节点内部暂停图，向外部发布一个 `Block`，等待人工、工具或其他系统给出结果后继续执行。

该机制与 `NodeGraph.stream()` 相互独立：中断通过事件系统发布，流式 chunk 通过 `StreamWriter` 发布。

## 核心流程

1. 节点调用 `await interrupt(data=...)`。
2. 运行时创建一个 `Block`。
3. 运行时发布 `get_graph_interrupted_name(graph.namespace)` 对应的事件，其 `event.context` 为该 `Block`。
4. interruption hook 收到 `Block` 并把它交给外部决策流程。
5. 外部调用 `block.resolve(value)`。
6. `interrupt()` 返回 value，节点从暂停点继续执行。

如果外部调用 `block.cancel()`，当前图 attempt 会回到当前节点或并发批次执行前的最新快照并进入 `aborted`。

每个图在构造时都会默认注册中断处理器，负责未处理 Block 的检查，以及错误、accepted 和取消通知的收尾。
默认处理器的名称和订阅事件名均由 `get_graph_interrupted_name(graph.namespace)` 生成，即 `__graph_interrupted___{namespace}`，随图分解而注销。

如果默认处理器执行核心函数时，`event.seen` 中只有它自己的名称，即此前没有其他 handler 开始执行核心函数，它会使 `await interrupt()` 抛出
`BlockHookNotRegisteredError`，不会无限等待。异常定义于
`apixis.core.utils.exception`，也可从 `apixis.core.utils` 导入。节点没有捕获该异常时，
图进入 `failed`，`invoke()` 和 `stream()` 向调用方传播同一个异常。

```python
from apixis.core.utils import BlockHookNotRegisteredError

try:
    decision = await interrupt(data="review")
except BlockHookNotRegisteredError:
    decision = "skip review"
```

默认处理器的优先级为 `0`，用户 hook 默认优先级为 `1`。判断依据是本次事件的 `seen` 执行记录，
不再扫描注册表，也不要求已执行的 handler 必须是 `BlockEventHandler`。
默认处理器自己的名称也会在核心函数调用前加入 `seen`，因此仅有自身记录仍表示无人接手。

| 默认处理器执行前的情况 | 结果 |
| --- | --- |
| 其他 handler 已执行核心函数，无错误且事件未 accepted | 允许 Block 继续等待；hook 可以将它交给外部后返回 |
| 普通 `subscribe()` 观察者已执行核心函数 | 同样计入 `seen`，不再触发缺失 hook 异常 |
| handler 执行后注销自己 | 本次 `seen` 保留；下次新事件重新判断 |
| handler 仅注册但被过滤，或排在默认处理器之后 | 不算已执行，无法阻止默认报错 |
| 前置 handler 失败、接受事件或传播取消 | 由错误、accepted 或取消通知结束等待，保留原来的终止结果 |

`seen` 不是 Block 完成状态。只有观察者执行过、但无人 `resolve()`、`cancel()` 或 `fail()` 时，
Block 仍会等待外部处理；`timeout=None` 时可能一直等待。后台 handler 也会记录 `seen`，但是否先于默认处理器开始执行取决于调度，
不适合作为确定的接手机制。已完成的 Block 不会被后续通知改写。

## 推荐用法：图拥有的 hook

```python
import asyncio

from apixis.core.graph import GraphManager, START
from apixis.core.graph.interrupter import Block, interrupt


pending_reviews: asyncio.Queue[Block] = asyncio.Queue()


async def review_document(state: dict) -> dict:
    decision = await interrupt(
        data={
            "document_id": state["document_id"],
            "summary": state["summary"],
        }
    )
    return {"decision": decision}


graph = (
    GraphManager()
    .add_node(review_document)
    .add_edge(START, "review_document")
    .compile_graph(using_namespace="document-review")
)


@graph.add_interrupted_hook
async def capture_review(block: Block) -> None:
    await pending_reviews.put(block)


async def run() -> dict:
    context = graph.create_context({
        "document_id": "doc-1",
        "summary": "Draft summary",
    })
    invocation = asyncio.create_task(graph.invoke(graph_context=context))

    block = await pending_reviews.get()
    block.resolve("approved")
    return await invocation
```

`graph.add_interrupted_hook` 自动选择图 namespace，并将 hook 纳入图生命周期。`graph.decompose()` 时会永久删除它，适合绝大多数场景。运行时产生的 Block.graph_id 标识所属图；图级 hook 通过图管理的 context 校验 graph_id、run_id 和 active 状态。attempt 结束后未完成的 Block 会关闭，旧中断事件不会转交给同 namespace 的替代图。

## 全局 interrupted_hook

如果 hook 生命周期不属于某个 `NodeGraph`，可以直接注册：

```python
from apixis.core.graph.interrupter import Block, interrupted_hook


@interrupted_hook(namespace="document-review", exist_ok=False)
async def on_document_review(block: Block) -> None:
    ...
```

对应事件名为：

```text
__graph_interrupted___document-review
```

独立 hook 的全局 namespace（`None`、空字符串或 `GLOBALNS`）对应：

```text
__graph_interrupted___<global>
```

图本身只有显式使用 `compile_graph(using_namespace=GLOBALNS)` 时才属于该全局命名域；省略 `using_namespace` 会为图自动生成唯一 namespace。

直接注册的 hook 不由图清理。卸载时使用：

```python
from apixis.core.event import unsubscribe

unsubscribe(on_document_review.__name__)
```

## interrupt()

```python
await interrupt(
    *,
    data: Any = None,
    timeout: float | None = None,
    context: GraphContext | None = None,
) -> Any
```

参数：

| 参数 | 说明 |
| --- | --- |
| `data` | 发送给 hook 的任意本地对象，保存于 `Block.with_data` |
| `timeout` | 最大等待秒数；通过默认处理器的 `seen` 检查后，`None` 无限等待；超时返回 `None` |
| `context` | 可选 active `GraphContext`；节点内省略时自动读取当前 context |

节点外省略 `context` 会抛出 `RuntimeError`。即使显式传入 context，它也必须仍处于 active 调用中。

`interrupt()` 本身不创建额外快照。Graph Runtime 已在当前节点或并发批次执行之前自动保存快照，所以取消中断会回到本次调度之前的状态。

## Hook 错误与事件接受

`interrupted_hook()` 注册的事件处理器会响应前置状态：前置前台 handler 报错时，通过 `block.fail(GraphNodeError(...))` 将错误交给等待中的节点；前置 handler 接受事件时，通过 `block.cancel()` 进入现有图中止流程。两种状态同时存在时先处理错误，不覆盖已完成的 Block。

图默认注册的中断处理器也具备这些通知能力，所以没有用户 hook 时，前置插件的失败、接受或取消仍可结束等待；这些终止结果不会被缺失 hook 异常覆盖。

用户 hook 自己抛出的异常由事件系统记录后交给 `on_error(event, error)`，再通过 `block.fail(error)` 传给等待中的节点；不会回调自己的 `on_has_error`。节点仍可以用 `try/except` 自行处理 `await interrupt()` 收到的异常；未捕获时按现有节点失败流程结束图。只有 `interrupt(timeout=...)` 自身的等待期限到达才返回 `None`，hook 传回的 `TimeoutError` 不会被当作等待超时吞掉。

前置前台插件或中断 hook 传播 `CancelledError` 时，事件处理器先将取消追加到 `event.error_stack`，再重新抛出；不会因取消调用 `on_error`。事件循环的 `on_cancelled` 通知向等待的 Block 传递取消异常，节点继续传播后，图调用也抛出 `CancelledError`。这条路径不会转换成 `interrupt()` 的超时返回，也不会被当作人工 `Block.cancel()` 而正常返回快照；等待中的 Block 和图调用均能结束。

默认处理器始终提供这些通知，用户通过 `interrupted_hook()` 或 `graph.add_interrupted_hook()` 注册的 hook 也提供相同的生命周期处理。后台 handler 的普通异常只写日志，取消不写入事件错误栈；取消清理回调自身的失败也不追加记录。

## Block

`Block` 是可 await 的冻结 dataclass：

| 成员 | 说明 |
| --- | --- |
| `run_id` | 所属图调用 attempt |
| `block_id` | 当前中断点唯一 id |
| `namespace` | 所属图 namespace |
| `with_data` | `interrupt(data=...)` 传入的数据 |
| `done` | Future 是否已经完成 |
| `cancelled` | Future 是否被取消 |
| `resolve(result)` | 让节点以 result 继续 |
| `cancel()` | 取消 Future，并触发当前 attempt abort |
| `fail(error)` | 以异常结束 Future，将异常传回等待 `Block` 的节点 |

`resolve()`、`fail()` 和 `cancel()` 都是一次性操作。Future 已完成后再次调用不改变原结果。

应用在外部保存 Block 时，建议以 `(run_id, block_id)` 为唯一键，而不是只按 namespace 或 data 查找。

## 多次中断

同一个节点可以顺序调用多次：

```python
async def staged_review(state: dict) -> dict:
    first = await interrupt(data={"stage": 1})
    second = await interrupt(
        data={"stage": 2, "first_result": first}
    )
    return {"review_results": [first, second]}
```

每次调用生成不同 `block_id`，但属于同一 attempt，因此 `run_id` 相同。hook 必须逐个 resolve 对应 Block。

## 超时

```python
decision = await interrupt(
    data={"question": "continue?"},
    timeout=30,
)
```

30 秒内未 resolve 时：

- `interrupt()` 返回 `None`；
- Block 的 Future 被取消，`block.cancelled` 为 `True`；
- 图不会因为中断 timeout 自动 abort，而是从 `interrupt()` 后继续执行；
- 后续 `block.resolve(...)` 是 no-op。

如果 `None` 也是合法业务值，应在协议层让外部返回带状态的对象，或让节点把 timeout 的 `None` 显式转换成业务状态。

## 外部取消

```python
block.cancel()
```

外部取消的完整效果：

1. `Block` Future 被取消；
2. `interrupt()` 捕获该外部取消；
3. 所属 `GraphContext.abort()` 被调用；
4. 当前节点立即因 `CancelledError` 停止；
5. 下游节点不会运行；
6. `invoke()` 返回当前节点或并发批次执行前的最新快照状态。

运行时自身取消任务（例如节点 timeout、stream 消费者退出）与外部 `Block.cancel()` 会被区分，不会被误当作一次人工取消。节点 timeout 仍按 `TimeoutError` 向调用方传播。

## 与 stream() 组合

```python
async def review(state: dict) -> dict:
    writer = get_stream_writer()
    writer({"type": "review_requested"})
    decision = await interrupt(data=state["proposal"])
    writer({"type": "review_resolved", "decision": decision})
    return {"decision": decision}
```

stream 与 interrupt 的消费者不同：

- `async for chunk in graph.stream(...)` 消费 writer chunk。
- interruption hook 消费 `Block`。

两者可以同时工作。若 Block 被取消导致 abort，取消之前已经写入 StreamChannel 的 chunk 会先被迭代器产出，然后流结束。

## 生产环境建议

- hook 中不要长时间阻塞事件处理器；把 Block 放入业务队列后尽快返回。
- 保存并校验 `run_id` 与 `block_id`，防止旧审批结果 resolve 新调用。
- 为人工操作设置合理 timeout，并明确 timeout 的业务含义。
- 图分解或应用关闭前，处理仍未完成的 Block，避免调用永久悬挂。
- 不要把 Block 通过远程事件通道发送；它包含进程内 Future，只适合本地事件系统。
- 使用 `graph.add_interrupted_hook` 管理图专属 hook，减少替换图后旧 hook 残留。

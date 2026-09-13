# GraphContext、快照、恢复与流式上下文

`GraphContext` 表示一次图调用尝试，持有最新已提交状态、调度目标、步数、生命周期、快照历史和本次运行的 Future、writer。

归属关系为 **namespace → graph → context**。namespace 只定位当前注册的图；context 创建时记录所属图的 graph_id，不持有 graph 对象。相同 namespace 的新图无法接纳旧 context 或恢复旧图快照。

## 创建与调用

快捷调用由图创建 context：

```python
result = await graph.invoke({"value": 1})
```

需要从外部 abort、检查快照或恢复时，由图准备 context：

```python
context = graph.create_context({"value": 1})
result = await graph.invoke(graph_context=context)

stream_context = graph.create_context({"value": 2})
async for chunk in graph.stream(graph_context=stream_context):
    print(chunk)
```

- `create_context(state)` 要求 state 为 dict。创建时复制普通字段，`KeepRef` 字段保留引用。
- `invoke()` 和 `stream()` 必须选择一个输入来源：state 或 graph_context。同时传入两者会抛出 TypeError。
- 传入已有 context 时，使用其 `state`，不再次传入或覆盖初始状态。
- 归属、pending 状态、恢复目标及步数限制在执行前校验；拒绝调用不会改写或消耗 context。
- 接受调用后再启动事件执行；启动或发布失败属于本次运行失败。
- `GraphContext` 仍可用于类型标注，但必须通过 graph 的创建、恢复接口取得实例。

## Schema 归属

schema 只在 `GraphManager(State)` 或 `NodeGraph(..., state_schema=State)` 配置。每次编译图时调用一次 `get_type_hints(..., include_extras=True)`，同时提取 `AutoMerge` 和 `KeepRef`，保存不可变的字段集合。

context 的创建、执行和恢复使用所属图已经解析的规则，不接受单独的 schema，不重复解析。修改原 schema 类的注解不会改变已编译图的行为；改变规则需要重新编译图。

## 生命周期

公开类型别名 `GraphContextStatus` 为 `Literal["pending", "running", "failed", "aborted", "finished"]`。

| 当前状态 | 允许进入 | 含义 |
| --- | --- | --- |
| `pending` | `running`、`failed`、`aborted` | 图已创建 context，尚未接受执行；运行时也可标记准备失败 |
| `running` | `finished`、`failed`、`aborted` | 已绑定本次运行资源 |
| `finished` | 无 | 正常完成 |
| `failed` | 无 | 已接受的运行发生异常 |
| `aborted` | 无 | 调用被中止或取消 |

终态 context 不能重置或再次运行。恢复创建另一个 pending context。同一个 context 并发提交时只有一次可以被接受；不同 context 可以在同一图上并发执行。

状态转移和合法性校验统一由 `_transition_to()` 完成。重复通知同一个终态保持原结果，不能由一个终态切换到另一个终态。运行资源只在转移校验成功后绑定，拒绝重复绑定不会覆盖原 run_id、Future 或 writer。pending context 不需要 Future 即可 abort。

| 属性或字段 | 说明 |
| --- | --- |
| `graph_id` | 只读的所属图 ID，与快照一致；不反向引用图 |
| `status` | 当前生命周期状态 |
| `is_consumed` | 是否已经不能开始调用，即状态不再为 pending |
| `is_bound` | 是否具有 run id、completion 和 writer |
| `is_active` | 是否 running、运行资源完整且 completion 未完成 |
| `run_id` | 接受执行时分配的唯一运行 ID |
| `state` | 最新已提交状态 |
| `target_node_name` | 待执行节点或有序节点列表 |
| `steps` | 已完成并提交的节点调度批次数 |
| `context_snapshot` | 按顺序保存的历史；建议通过快照读取接口访问 |

`completion`、`stream_writer` 由运行时管理，不应直接修改。

## 快照时机与结构

每个普通节点、图级并发批次或内部 router/condition 节点执行前自动创建快照。START 和 END 不创建快照；并发批次产生一份批次级检查点。

```python
class GraphContextSnapshot(TypedDict):
    timestamp: float
    state: dict[str, Any]
    target_node_name: str | list[str]
    steps: int
    graph_id: str
```

- `graph_id` 是源图编译实例的身份，与只读属性 `graph.graph_id` 对应。
- `state` 是待执行节点之前的最后已提交状态。
- `target_node_name` 是恢复后应重新执行的单个节点或完整有序批次；空列表表示完成。
- `steps` 是此前已完成的步数。
- 快照始终深拷贝全部状态，包括 KeepRef 字段。

节点异常或超时发生在 Command 应用前时，本批不提交更新；按序应用 Command 时发生冲突或合并错误，live state 可能已有部分提交，恢复仍使用执行前快照。

## 读取快照

```python
latest = context.get_snapshot()
first = context.get_snapshot(0)
previous = context.get_snapshot(-2)
history = context.get_all_snapshots()
```

读取接口返回深拷贝，修改返回值不影响保存的历史。没有快照时 `get_snapshot()` 返回 None；版本越界沿用 Python list 的 IndexError。

`take_a_snapshot()` 只允许 active context 调用，通常由图自动执行。没有快照的 context 无法恢复。

## 在同一图上恢复

```python
context = graph.create_context(initial_state)
try:
    await graph.invoke(graph_context=context)
except Exception:
    recovered = graph.restore_context(context.get_all_snapshots())
    result = await graph.invoke(graph_context=recovered)
```

`graph.restore_context()` 接受单份快照、快照列表或所属图的 context：

```python
recovered = graph.restore_context(snapshot)
branch = graph.restore_context(snapshots, version=1)
recovered = graph.restore_context(context, version=-1)
```

列表版本使用原生索引：0 为首份，-1 为最新，-2 为倒数第二份；越界抛出 IndexError，非法索引类型抛出 TypeError。单份快照忽略 version。

恢复保留选中版本及之前的历史，丢弃之后的历史。所有保留检查点都校验结构和 graph_id，拒绝混入其他图的快照。

传入 context 时先校验其 graph_id，再从保存的历史中选择版本；不复制 context 的运行资源或当前 live state，也不提前复制将被丢弃的后续版本。

恢复后的 live state 与保留历史分别深拷贝，二者互不共享可变状态，即使本图使用 KeepRef 也不会污染旧检查点。恢复出的目标节点列表同样与历史独立。

新 context 属于原图、状态为 pending；run_id、Future、writer 不从旧尝试复制。快照的结构与归属校验、版本选择、复制和新 context 创建均由 graph.restore_context() 完成。目标节点和步数限制在调用入口检查。

## 图替换的边界

| 顺序 | 结果 |
| --- | --- |
| 创建 pending context 后替换图 | 原 context 被中止，graph_id 不变，不能交给新图执行 |
| 创建恢复 context 后替换图 | 恢复 context 被中止，不能迁移 |
| 替换图后恢复旧快照 | 拒绝，即使 namespace 和 schema 相同 |
| 旧 invocation 尚未退出时替换 | 先强制中止旧图 context、释放旧注册并接管 namespace，再注册新监听器 |
| abort 并等待 invocation 退出后替换 | 允许；旧节点迟到结果不能提交 |

替换仅改变 namespace 的注册入口。旧 context 和快照仍可查看，保留它们不会维持旧图的对象引用；它们不能由替代图恢复执行。新图必须通过自己的接口创建新 context。

旧版只有 namespace 的快照缺少 graph_id，会被明确拒绝。进程重启后重新编译也会产生新身份，因此当前恢复范围限定在同一存活图实例内。

## 图分解

```python
graph.decompose()              # Forcefully abort unfinished contexts.
graph.decompose(force=False)   # Reject if any managed context is unfinished.
```

默认 force=True：中止图管理的 pending 和 running context，释放 context 集合、图拥有的监听器和 namespace。已开始的调用按原 abort 语义返回最后检查点；stream 先排空已缓存 chunk。节点可能暂时继续运行，但不能提交状态或发布下一跳。

force=False：存在未结束 context 时抛出 RuntimeError，不修改任何 context 或注册。没有未结束 context 时正常分解。两种模式都幂等。

底层 `release_namespace(graph)` 默认触发强制分解，重复释放或释放旧图不会移除替代图的注册。`decompose_immediately=False` 仅释放该图的 namespace 注册，供分解内部清理或显式重新注册使用，不会终止 context 或移除监听器。`acquire_namespace(..., replace_existed=True)` 通过释放接口分解旧图；监听器注册仍由 NodeGraph 负责，在取得 namespace 后执行。注册失败时仅清理新图资源并抛出原始异常，不恢复旧图。

普通执行结束、失败或取消后，graph 会释放该次 context 的管理记录。调用方仍可保留 context 查看状态和快照。尚未执行的 context 由图保留；不再使用时可以调用 await graph.abort(context) 或分解图。

运行时取消与主动 `abort()` 都使用终态 `aborted`，但调用结果不同：前者取消 completion，使 `invoke()` / `stream()` 抛出 `CancelledError`；后者正常返回最新快照。事件取消通知不会覆盖已有终态。

## abort()

可以直接调用：

```python
context.abort()
```

也可以经由图验证所有权后调用：

```python
await graph.abort(context)
```

graph.abort() 也可以中止并释放该图管理的 pending context。active 调用 abort 后：

- `invoke()` 立即以最新快照状态完成；
- `stream()` 会先产出已经排队的 chunk，再结束；
- 当前节点可能在后台继续运行，但它的结果不会被提交，也不会继续路由；
- context 进入 `aborted`；
- abort 幂等；
- 所属中断 Block 会关闭，不会由替代图接管。

因为自动快照在节点或并发批次执行前创建，abort 返回的是当前节点或整批之前的状态。若还没有快照，则回退到当前 live state。存在快照时，graph 会深拷贝返回值以隔离历史。context 的内部 completion 只传递原状态对象，不能当作公开的结果复制接口；调用方应 await graph.invoke() 或消费 graph.stream()。

对尚未绑定的 pending context 调用 `abort()` 只把它置为 `aborted`，没有 completion 可返回；此 context 随后不能用于图调用。

`finished` 或 `failed` context 不能再 abort。

## 节点内访问当前上下文

Graph Runtime 在节点执行期间通过 `ContextVar` 绑定当前 `GraphContext`：

```python
from apixis.core.graph.context import (
    get_current_namespace,
    get_current_run_id,
    get_graph_context,
    get_stream_writer,
)


async def node(state: dict) -> dict:
    context = get_graph_context()
    run_id = get_current_run_id()
    namespace = get_current_namespace()
    writer = get_stream_writer()

    writer({"run_id": run_id, "namespace": namespace})
    return {}
```

这些函数只能在节点执行上下文中使用：

- 图外调用会抛出 `RuntimeError`。
- 同一个图级并发批次的所有节点读取到同一个 `GraphContext` 对象；context 在节点执行过程中按约定只读。
- 同一个 `NodeGraph` 的不同并发调用绑定各自的 context 和 run id，不会串流。
- `get_stream_writer()` 在 `invoke()` 中返回可复用的 no-op writer，因此节点可以无条件发出 chunk；只有 `stream()` 调用方会收到内容。
- 由节点创建的 asyncio task 会按照 Python `ContextVar` 规则继承创建时上下文，但任务不应在图 attempt 结束后继续使用 context 或 writer。

`apix_graph_context(context)` 是底层 context manager，主要供 Graph Runtime 或自定义 `BaseNode` 执行框架绑定上下文。普通节点无需手动使用。

## StreamWriter 与 StreamChannel

`StreamWriter` 是同步 callable：

```python
writer(chunk)
writer.write(chunk)
```

`StreamChannel` 是单消费者异步迭代器，并暴露 `.writer`：

```python
channel = StreamChannel()
channel.writer("first")
channel.close()

chunks = [chunk async for chunk in channel]
```

关闭行为：

- `close()` 幂等。
- 关闭前已经排队的 chunk 仍会按 FIFO 读取。
- 关闭后继续写入抛出 `RuntimeError`。
- writer 使用当前事件循环内的 `asyncio.Queue.put_nowait()`；不要从其他线程直接调用。

一般应用只使用 `NodeGraph.stream()` 和 `get_stream_writer()`，无需直接创建 `StreamChannel`。

`noop_stream_writer()` 返回全局可复用的丢弃型 writer，`NodeGraph.invoke()` 使用它统一节点接口。自定义运行器在不需要实际流式输出时也可以使用该函数。

## 快照持久化注意事项

快照可以存储和查看，但恢复仍要求同一存活图实例；持久化不提供跨进程或重新编译后的恢复能力。`state` 的具体值是否可序列化由应用负责：

- 内存恢复只要求对象支持 `deepcopy`。
- JSON、数据库或消息队列持久化还要求 state 值可由对应格式编码。
- Future、锁、打开的客户端和文件句柄通常不适合进入持久快照。
- 如果运行资源必须放在 state 中，可使用 `KeepRef` 避免节点 deepcopy，但自动快照仍会 deepcopy 它；应提供可复制表示或把可恢复描述与实时资源分离。

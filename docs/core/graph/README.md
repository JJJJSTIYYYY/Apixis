# Graph API

Graph API 的主要入口是 `GraphManager`。它负责声明节点与边，`compile_graph()` 返回可执行的 `NodeGraph`。

## 构建图

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
```

### `GraphManager(state_schema=None)`

可选 `state_schema` 用于声明 `AutoMerge` / `KeepRef` 等 state 策略。

### `add_node(node_func, node_name=None, *, timeout=None)`

注册同步/异步函数或 `BaseNode`。普通函数默认使用 `__name__` 作为节点名。

### `add_nodes(node_list)`

批量注册节点。

### `add_edge(l_node, r_node, condition=None, *, timeout=None)`

添加直接边。传入 `condition` 时，会插入一个条件节点；条件函数必须返回 `bool`，`True` 路由至 `r_node`，`False` 路由至 `END`。

### `add_router(l_node, r_nodes, router, *, timeout=None)`

添加路由节点。router 可返回单个目标名、目标名列表、`Command(goto=...)`，或包含 `goto` 的 mapping。目标必须来自 `r_nodes`。

### `compile_graph(using_namespace=None, exist_ok=False)`

编译为 `NodeGraph`。图必须存在 `START` 的出边。

- `using_namespace=None` 或 `""`：自动生成 namespace；
- `GLOBALNS`：显式使用全局命名域；
- `exist_ok=False`：namespace 已占用时抛异常；
- `exist_ok=True`：释放旧图并由新图接管。

## 执行

### `await graph.invoke(state=None, graph_context=None)`

两种调用方式二选一：

```python
result = await graph.invoke({"value": 1})
```

或：

```python
context = graph.create_context({"value": 1})
result = await graph.invoke(graph_context=context)
```

不能同时传 `state` 和 `graph_context`。

## Stream

```python
async for chunk in graph.stream({"value": 1}):
    ...
```

节点内部可通过：

```python
from apixis import get_stream_writer

writer = get_stream_writer()
writer.write({"progress": 0.5})
```

向图外实时发送数据。

## `GraphContext`

推荐通过图创建，而不是直接实例化：

```python
context = graph.create_context(initial_state)
```

常用属性：

- `graph_id`
- `status`: `pending | running | failed | aborted | finished`
- `state`
- `target_node_name`
- `steps`
- `is_consumed`
- `is_bound`
- `is_active`

### 快照

```python
context.take_a_snapshot()
latest = context.get_snapshot()
all_snapshots = context.get_all_snapshots()
```

恢复由图负责：

```python
restored = graph.restore_context(context)
restored = graph.restore_context(snapshot)
restored = graph.restore_context(snapshot_list, version=-1)
```

恢复结果属于同一个 `NodeGraph`，不能跨图使用。

## Abort

```python
await graph.abort(context)
# or
context.abort()
```

abort 使用最近一次 checkpoint 作为返回边界。已经开始执行的节点可能继续完成自身协程，但其结果不会再提交到已中止 invocation。

## Interrupt / Block

节点内：

```python
from apixis import interrupt

answer = await interrupt(data={"question": "continue?"})
```

为图注册处理器：

```python
@graph.add_interrupted_hook
async def on_interrupted(block):
    block.resolve("yes")
```

也可以按 namespace 注册：

```python
from apixis import interrupted_hook

@interrupted_hook(graph.namespace)
async def on_interrupted(block):
    block.resolve("yes")
```

如果图发送了 `Block` 但没有可执行的 interruption hook，默认 handler 会抛出 `BlockHookNotRegisteredError`，避免 invocation 永久挂起。
如果 hook 正常返回后，`Block` 仍未完成且没有调用 `block.accept()`，默认 handler 会抛出 `BlockNotResolvedError`。需要暂存 Block、稍后由外部响应时，必须在 hook 返回前调用 `block.accept()`；它只声明接管，不会恢复图执行，仍需稍后调用 `resolve()`、`fail()` 或 `cancel()`。hook 自身抛出的异常会传播到等待中断的节点。

`Block` 常用接口：

```python
block.resolve(value) # 向图内 interrupt 中断处回传结果
block.fail(error) # 向图内 interrupt 中断处回传 error 并中断图的后续调度
block.cancel() # 仅中断图的后续调度
block.accept() # 标记 block 已被处理，可用于 block 暂存后的异步处理逻辑
```

## 生命周期

编译后的图持有 namespace 和 event handler 注册。使用结束后调用：

```python
graph.decompose()
```

`decompose(force=True)` 会中止未完成 context 并释放 handler / namespace；`force=False` 在仍有未完成 context 时抛异常。

### 使用上下文管理器自动管理生命周期

```python
with graph:
    await graph.invoke({...})
```

上下文管理器退出时自动分解图。
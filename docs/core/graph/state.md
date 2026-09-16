# Graph state & routing

## 节点返回值

普通 `Node` 函数必须返回：

- `dict` / mapping：作为 state update；或
- `Command`：同时描述 update 与下一跳。

```python
from apixis import Command


def step(state):
    return Command(
        update={"count": state["count"] + 1},
        goto="next",
    )
```

普通 `Node` 不接受 `list[Command]`。`ParallelNode` 等专用 `BaseNode` 可返回多个 command，由图按声明顺序应用。

## `Command`

```python
Command(
    update: dict = {},
    goto: str | list[str] | None = None,
)
```

`goto` 语义：

- `None`：使用 manager 定义的默认下一跳；
- `"node"`：路由到单节点；
- `["a", "b"]`：并发执行一个节点 batch；
- `[]`：立即结束图。

## `AutoMerge`

在 schema 中使用 `Annotated`：

```python
from typing import Annotated, TypedDict
from apixis import AutoMerge

class State(TypedDict):
    messages: Annotated[list[str], AutoMerge()]
```

当字段已有值时，更新通过当前值的 `__add__` 合并；字段不存在时直接写入。

## `Reset`

用于单次绕过 `AutoMerge`：

```python
from apixis import Reset

return {"messages": Reset([])}
```

最终 state 中保存的是 `Reset.value`，而不是 wrapper 本身。

## `KeepRef`

```python
from typing import Annotated
from apixis import KeepRef

resource: Annotated[object, KeepRef()]
```

被标记字段在节点执行前的 state copy 中保留原对象引用，其余字段正常复制。

`KeepRef` 不表示整个 invocation 生命周期永久使用同一个 state 对象；它只改变图执行边界上的字段复制策略。并行执行时共享可变引用可能产生竞争，应由调用方自行同步。

## `ParallelNode`

```python
from apixis import ParallelNode

parallel = ParallelNode([branch_a, branch_b], name="parallel")
manager.add_node(parallel)
```

每个 branch 接收同一个节点局部 state snapshot，branch 并发执行，结果按 branch 声明顺序合并，而不是按完成顺序合并。

## 并发下一跳

任意节点可返回：

```python
Command(goto=["a", "b"])
```

这表示下一批次并发执行 `a`、`b`。结果应用顺序仍按列表顺序确定。

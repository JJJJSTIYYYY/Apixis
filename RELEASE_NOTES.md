# Context 运行期管理

- `create_context()` / `restore_context()` 只准备 pending context，不再让图持有它。
- `invoke()` / `stream()` 接纳调用并进入 running 后建立管理关系，完成、失败、中止或取消时立即解除；直接调用 `context.abort()` 同样生效。
- `decompose(force=True)` 仅中止 running context；`force=False` 仅在存在 running context 时拒绝分解。
- pending context 不因图分解或命名空间替换而改变状态；之后通过已分解的图执行时抛异常，跨图调用仍被拒绝。
- 对齐接口文档与生命周期测试，新增未执行 context 的状态资源回收、直接 abort 的即时清理，以及尚未消费完的 stream 终结清理回归。

本轮验证：Python 3.12.14、pytest 9.1.1、pytest-asyncio 1.4.0，完整测试 **901 passed**。

# Graph Command 重构

- 删除 `GraphManager.add_edge()`、`add_router()`、条件节点生成逻辑和默认跳转表。
- `Command.goto` 是节点执行后唯一的下一跳来源：`None` / `[]` 结束当前分支，字符串执行单节点，列表执行并发步。
- 并发步按声明顺序合并 command，下一跳按首次出现顺序去重。结束分支不阻止其他分支继续，所有分支均无下一跳时结束图。
- 单元素列表保留并发步形式；专用节点的空 command 列表不再生成占位 command。
- 删除 `START` 及其额外调度；`END` 改为不公开导出的内部标记 `_END`。
- 仅新建 context 时复制入口目标；快照恢复保留目标节点与步数，执行始终以 context 当前目标为准。
- 同步迁移调用示例、安装验证脚本、测试与接口文档。

## 接口迁移

```python
from apixis import Command, GraphManager


def first(state):
    return Command(update={"value": state["value"] + 1}, goto="second")


def second(state):
    return {"value": state["value"] * 2}


graph = GraphManager().add_nodes([first, second]).compile_graph("first")
```

`compile_graph(entry_point, *, using_namespace=None, exist_ok=False)` 必须显式指定入口。
直接构造使用 `NodeGraph(nodes, entry_point, ...)`；不再接受默认跳转表。
`validate_graph_definition(nodes)` 仅验证节点定义。
条件判断放在普通节点内，由其返回 `Command(goto=...)`。
普通 mapping 返回值只更新 state 并结束当前分支；需要继续时必须显式返回 `Command`。

## 本轮验证

Python 3.12.14、pytest 9.1.1、pytest-asyncio 1.4.0：完整测试 **892 passed**。
远程通道沿用模拟测试，未连接真实 Kafka / RabbitMQ 服务。

---

以下为此前版本的发布记录。

# 本次修复与发布验证

## 修改内容

- 修复背压测试的运行时注入：同步与异步 getter 使用同一个独立 event core，并完整清理任务与通道。
- 按现有中断协议修复延迟响应测试：hook 返回前调用 `block.accept()`；增加 7 个未接管 Block 的报错用例，覆盖不同注册方式、通配符及自行注销。
- 对齐生命周期测试：停止时离线广播失败仍保留停止状态；`start_core()` 按启动信号语义验证，不要求等待所有启动任务结束。
- 清理测试中的旧 Python 3.10 兼容代码；项目最低版本仍为 Python 3.12。
- 将 `pyproject.toml`、`uv.lock`、`.python-version` 统一到仓库根目录，保留 `source/apixis` 源码布局。
- 配置 setuptools 构建后端、运行依赖、可选开发依赖、包发现范围、类型标记及许可证文件。
- 版本统一为原打包配置中的 `0.1.0`，由 `source/apixis/_version.py` 维护；保留 `VERSION` 并增加 `__version__`。
- wheel 仅分发运行包及元数据；源码包额外包含文档、测试、锁文件和安装验证脚本。
- 保留内置 IdGenerator 的第三方声明，并随发行包附带 MIT 许可证。
- 更新 CI，在 Python 3.12 / 3.13 / 3.14 上运行测试、构建、元数据检查及发行包安装验证。
- 补充 `docs/releasing.md`，修正 `Block.accept()` 的说明：声明接管不会恢复图执行。

本次没有更改事件调度、图执行或中断处理的业务逻辑。

## 本地验证结果

环境：Python 3.12.14、pytest 9.1.1、pytest-asyncio 1.4.0。

| 验证 | 结果 |
| --- | --- |
| 修改前完整测试 | 807 通过、16 失败、16 个初始化错误 |
| 修改后开发安装完整测试 | 846 通过 |
| wheel 通过 pip 安装后的完整测试 | 846 通过 |
| 源码包通过 pip 安装后，运行源码包自带测试 | 846 通过 |
| 隔离构建源码包，再从源码包构建 wheel | 通过 |
| `twine check --strict`，wheel 与源码包 | 均通过 |
| `pip check` | 无依赖冲突 |
| 安装位置、版本、类型标记、许可证检查 | 通过 |
| 安装后事件发布与图执行验证 | wheel 与源码包均通过 |
| `uv lock --check` | 通过 |

发行包测试使用 `python -I` 和独立虚拟环境，未通过 `PYTHONPATH` 或 pytest `pythonpath` 导入工作区源码。
本地实际验证了 Python 3.12；3.13、3.14 已配置到 CI，尚未在本环境运行。远程通道测试使用现有模拟实现，没有连接真实 Kafka / RabbitMQ 服务。

## 使用方式

解压后进入项目根目录：

```bash
python -m pip install .
```

预构建发行文件位于 `dist/`。需要重新运行测试或构建：

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m build
python -m twine check --strict dist/*
```

尚未上传 PyPI。确认账号拥有 `apixis` 名称的发布权限后，按 `docs/releasing.md` 完成上传。

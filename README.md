<a id="chinese"></a>

<div align="center">

# APIXIS

中文 | [English](#english)

面向 Python asyncio 应用的事件驱动图编排。

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2B%20MIT-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)

[源码](https://github.com/JJJJSTIYYYY/Apixis) ·
[问题](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[API 参考](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)

</div>

APIXIS 提供两个可组合的运行时：

- 一个异步事件系统，支持通配符订阅、有序处理器、背压和可插拔通道；
- 一个有状态图运行时，支持命令驱动的步骤、并行节点、快照、流式处理以及中断/恢复工作流。

> 需要 Python 3.12+。运行时 API 专为 `asyncio` 设计。

## 安装

```bash
pip install apixis
```

要安装本地检出，请在仓库根目录运行：

```bash
python -m pip install .
```

如需开发和发布工具：

```bash
python -m pip install -e ".[dev]"
python -m pytest
```

使用 uv 时，请使用 `uv sync --extra dev --locked` 和 `uv run --extra dev pytest`。
有关构建和发布到 PyPI，请参阅[发布指南](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/releasing.md)。

## 快速开始

```python
import asyncio

from apixis import GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    try:
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 2}
    finally:
        graph.decompose()


asyncio.run(master())
```

## 文档

- [API 参考](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)
- [事件 API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/event/README.md)
- [图 API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/README.md)
- [状态与命令](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/state.md)
- [工具与异常](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/utils/README.md)

## 许可证

根据 Apache License 2.0 授权。请参阅 [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)。
捆绑的 IdGenerator 组件采用 MIT 许可证；请参阅[第三方声明](https://github.com/JJJJSTIYYYY/Apixis/blob/master/THIRD_PARTY_NOTICES.md)。

---

<a id="english"></a>

<div align="center">

# APIXIS

[English] | [中文](#chinese)

Event-driven graph orchestration for Python asyncio applications.

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2B%20MIT-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)

[Source](https://github.com/JJJJSTIYYYY/Apixis) ·
[Issues](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[API Reference](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)

</div>

APIXIS provides two composable runtimes:

- an asynchronous event system with wildcard subscriptions, ordered handlers, backpressure, and pluggable channels;
- a stateful graph runtime with Command-driven steps, parallel nodes, snapshots, streaming, and interruption/resume workflows.

> Python 3.12+ is required. Runtime APIs are designed for `asyncio`.

## Install

```bash
pip install apixis
```

To install a local checkout, run from the repository root:

```bash
python -m pip install .
```

For development and release tools:

```bash
python -m pip install -e ".[dev]"
python -m pytest
```

With uv, use `uv sync --extra dev --locked` and `uv run --extra dev pytest`.
See [the release guide](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/releasing.md) for building and publishing to PyPI.

## Quick start

```python
import asyncio

from apixis import GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    try:
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 2}
    finally:
        graph.decompose()


asyncio.run(master())
```

## Documentation

- [API reference](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)
- [Event API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/event/README.md)
- [Graph API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/README.md)
- [State and commands](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/state.md)
- [Utilities and exceptions](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/utils/README.md)

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE).
The bundled IdGenerator component is MIT licensed; see [third-party notices](https://github.com/JJJJSTIYYYY/Apixis/blob/master/THIRD_PARTY_NOTICES.md).
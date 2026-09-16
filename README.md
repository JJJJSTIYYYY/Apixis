<div align="center">

# APIXIS

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
- a stateful graph runtime with routing, parallel nodes, snapshots, streaming, and interruption/resume workflows.

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

from apixis import END, START, GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    graph = (
        GraphManager()
        .add_node(increment)
        .add_edge(START, "increment")
        .add_edge("increment", END)
        .compile_graph()
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
- [State and routing](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/state.md)
- [Utilities and exceptions](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/utils/README.md)

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE).
The bundled IdGenerator component is MIT licensed; see [third-party notices](https://github.com/JJJJSTIYYYY/Apixis/blob/master/THIRD_PARTY_NOTICES.md).
<div align="center">

# APIXIS

Event-driven graph orchestration for Python asyncio applications.

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
See [the release guide](./docs/releasing.md) for building and publishing to PyPI.

## Quick start

```python
import asyncio

from apixis import END, START, GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def main() -> None:
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


asyncio.run(main())
```

## Documentation

- [API reference](./docs/README.md)
- [Event API](./docs/core/event/README.md)
- [Graph API](./docs/core/graph/README.md)
- [State and routing](./docs/core/graph/state.md)
- [Utilities and exceptions](./docs/core/utils/README.md)

## License

Licensed under the Apache License 2.0. See [LICENSE](./LICENSE).
The bundled IdGenerator component is MIT licensed; see [third-party notices](./THIRD_PARTY_NOTICES.md).

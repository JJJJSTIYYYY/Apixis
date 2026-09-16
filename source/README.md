# APIXIS

Event-driven graph orchestration for Python asyncio applications.

APIXIS includes an asynchronous event runtime and a stateful graph runtime. It requires Python 3.12 or newer.

## Install

```bash
pip install apixis
```

## Graph example

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
        print(await graph.invoke({"value": 1}))
    finally:
        graph.decompose()


asyncio.run(main())
```

## Event example

```python
import asyncio

from apixis import EventType, get_event_pipe, start_core, subscribe, unsubscribe


@subscribe("demo.*")
async def receive(event) -> None:
    print(event.event_name, event.context)


async def main() -> None:
    await start_core()
    pipe = get_event_pipe()
    await pipe.post_event(
        event_type=EventType.INFO,
        event_name="demo.ready",
        context={"ok": True},
    )
    await pipe.join()
    unsubscribe("receive")


asyncio.run(main())
```

Full documentation is available in the project repository under `docs/`.

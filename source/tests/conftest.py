"""Shared pytest isolation for process-global runtime registries."""

import asyncio

import pytest
import pytest_asyncio

from apixis.core.event.factory import (
    start_core,
    get_handler_registry,
    get_event_loop,
    get_event_pipe,
)
from apixis.core.graph.base import GRAPH_DISPATCH, _namespace_graphs


def _clear_node_graph_listeners() -> None:
    """Remove listeners registered by NodeGraph instances from prior tests."""
    for graph in tuple(_namespace_graphs.values()):
        graph.decompose()
    _namespace_graphs.clear()

    handler_names = {
        name
        for name in get_handler_registry().registry
        if name.startswith(f"{GRAPH_DISPATCH}_")
    }
    for handler_name in handler_names:
        get_handler_registry().unregister_handler(handler_name)


@pytest.fixture(autouse=True)
def isolate_node_graph_listeners():
    """Give every test a clean global NodeGraph listener namespace."""
    _clear_node_graph_listeners()
    yield
    _clear_node_graph_listeners()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup_event_runtime(isolate_node_graph_listeners):
    """Release test-owned tasks explicitly now that stop only halts consumption."""
    await start_core()
    event_loop, event_pipe = get_event_loop(), get_event_pipe()
    yield
    # Reuse captured components so cleanup does not schedule a restart.
    await event_loop.stop()
    tasks = list(event_loop._dispatch_tasks | event_loop._background_handler_tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await event_pipe.clear()
    await event_pipe.stop()

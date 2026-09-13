"""Every compiled graph owns interruption cleanup and a missing-hook fallback."""

import asyncio

import pytest

from apixis.core.event import (
    EVENT_PIPE,
    get_handler,
    subscribe,
    unsubscribe,
)
from apixis.core.graph import END, GLOBALNS, START, GraphManager
from apixis.core.graph.interrupter import interrupt, interrupted_hook
from apixis.core.utils import BlockHookNotRegisteredError
from apixis.core.utils.exception import GraphNodeError


async def run_graph(graph, context, mode):
    if mode == "invoke":
        return await graph.invoke(graph_context=context)
    return [chunk async for chunk in graph.stream(graph_context=context)]


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("namespace", [None, GLOBALNS, "missing-hook"])
@pytest.mark.parametrize("timeout", [None, 10])
async def test_missing_hook_fails_without_waiting_for_timeout(mode, namespace, timeout):
    continued = []

    async def review(state):
        await interrupt(data="review", timeout=timeout)
        continued.append(True)
        return state

    graph = (GraphManager().add_node(review).add_edge(START, "review")
             .compile_graph(namespace))
    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError, match=graph.namespace):
                await run_graph(graph, context, mode)
            await EVENT_PIPE.join()
        assert context.status == "failed"
        assert context.completion.done()
        assert continued == []
    finally:
        graph.decompose()


@pytest.mark.parametrize("registration", ["owned", "standalone_before", "standalone_after"])
async def test_default_allows_hook_to_defer_response_until_external_resolution(registration):
    blocks = asyncio.Queue()
    namespace = "deferred-review"

    async def capture(block):
        await blocks.put(block)

    async def review(state):
        return {"answer": await interrupt()}

    if registration == "standalone_before":
        interrupted_hook(namespace)(capture)
    graph = (GraphManager().add_node(review).add_edge(START, "review")
             .compile_graph(namespace))
    if registration == "owned":
        graph.add_interrupted_hook(capture)
    elif registration == "standalone_after":
        interrupted_hook(namespace)(capture)

    context = graph.create_context({})
    task = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        async with asyncio.timeout(1):
            block = await blocks.get()
            # Allow the default handler to run after capture() has returned.
            await asyncio.sleep(0)
            assert not block.done
            assert not task.done()
            block.resolve("approved")
            assert await task == {"answer": "approved"}
            await EVENT_PIPE.join()
        unsubscribe(capture.__name__)
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await EVENT_PIPE.join()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        unsubscribe(capture.__name__)
        graph.decompose()


async def test_plain_observer_does_not_count_as_registered_block_hook():
    blocks = []

    async def review(state):
        await interrupt()
        return state

    graph = GraphManager().add_node(review).add_edge(START, "review").compile_graph()

    @subscribe(f"graph_{graph.namespace}_interrupted", priority=10)
    async def observe(event):
        blocks.append(event.context)

    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await EVENT_PIPE.join()
        assert len(blocks) == 1 and blocks[0].done
    finally:
        unsubscribe(observe.__name__)
        graph.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("action", ["error", "accept", "cancel"])
async def test_default_handles_upstream_termination_without_user_hook(mode, action):
    blocks = []
    continued = []

    async def review(state):
        await interrupt()
        continued.append(True)
        return state

    graph = GraphManager().add_node(review).add_edge(START, "review").compile_graph()

    @subscribe(f"graph_{graph.namespace}_interrupted", priority=10)
    async def plugin(event):
        blocks.append(event.context)
        if action == "error":
            raise ValueError("plugin failed")
        if action == "cancel":
            raise asyncio.CancelledError()
        event.accept()

    context = graph.create_context({"value": "initial"})
    try:
        async with asyncio.timeout(1):
            if action == "accept":
                result = await run_graph(graph, context, mode)
                assert result == ({"value": "initial"} if mode == "invoke" else [])
            else:
                error = GraphNodeError if action == "error" else asyncio.CancelledError
                with pytest.raises(error):
                    await run_graph(graph, context, mode)
            await EVENT_PIPE.join()
        assert context.status == ("failed" if action == "error" else "aborted")
        assert context.completion.cancelled() is (action == "cancel")
        assert len(blocks) == 1 and blocks[0].done
        assert continued == []
    finally:
        unsubscribe(plugin.__name__)
        graph.decompose()


async def test_node_can_recover_from_missing_hook_error():
    async def review(state):
        try:
            await interrupt()
        except BlockHookNotRegisteredError:
            return {"review_skipped": True}

    graph = GraphManager().add_node(review).add_edge(START, "review").compile_graph()
    try:
        async with asyncio.timeout(1):
            assert await graph.invoke({}) == {"review_skipped": True}
            await EVENT_PIPE.join()
    finally:
        graph.decompose()


async def test_default_registration_collision_releases_partial_graph_and_namespace():
    event_name = "graph_collision_interrupted"

    async def unrelated(event):
        pass

    # Occupy the default handler's public event name to force the second
    # listener registration to fail after graph dispatch has been registered.
    unrelated.__name__ = event_name
    subscribe(event_name)(unrelated)
    manager = GraphManager().add_edge(START, END)
    try:
        from apixis.core.utils.exception import EventHandlerAlreadyRegisteredError
        with pytest.raises(EventHandlerAlreadyRegisteredError):
            manager.compile_graph("collision")
        assert get_handler(event_name).core_func is unrelated
    finally:
        unsubscribe(event_name)

    graph = manager.compile_graph("collision")
    async with asyncio.timeout(1):
        assert await graph.invoke({}) == {}
    graph.decompose()


async def test_decomposition_removes_default_and_namespace_reuse_restores_it():
    async def review(state):
        await interrupt()
        return state

    manager = GraphManager().add_node(review).add_edge(START, "review")
    old = manager.compile_graph("reused-default")

    @old.add_interrupted_hook
    async def resolve(block):
        block.resolve("old answer")

    old.decompose()
    graph = manager.compile_graph("reused-default")
    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await EVENT_PIPE.join()
    finally:
        graph.decompose()

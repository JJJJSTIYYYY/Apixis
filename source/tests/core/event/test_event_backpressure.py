"""Behaviour regressions for ready admission and processing backpressure."""

import asyncio

import pytest

from apixis.core.event import ApixEvent, EventType, subscribe, unsubscribe
from apixis.core.event import event_loop, event_pipe
from apixis.core.event.handler_registry import APIX_HANDLER_REGISTRY
from apixis.core.graph import START, END, GraphManager
from apixis.core.graph import node_graph


@pytest.fixture
async def runtime(monkeypatch):
    """Exercise real publication with two dispatch permits and a one-item buffer."""
    for name in tuple(APIX_HANDLER_REGISTRY.registry):
        unsubscribe(name)
    monkeypatch.setattr(event_loop, "EVENT_LOOP_BACKPRESSURE", 2)
    monkeypatch.setattr(event_loop, "EVENT_PIPE_MAX_LEN", 1)
    monkeypatch.setattr(event_loop, "BACKGROUND_HANDLER_BACKPRESSURE", 1)
    monkeypatch.setattr(event_loop, "SHOW_EVENT_DISPATCH", False)
    monkeypatch.setattr(event_pipe, "EVENT_PIPE_MAX_LEN", 1)
    pipe = event_pipe.ApixEventPipe(remote_enabled=False)
    loop = event_loop.ApixEventLoop(APIX_HANDLER_REGISTRY)
    monkeypatch.setattr(event_loop, "EVENT_PIPE", pipe)
    monkeypatch.setattr(event_pipe, "EVENT_PIPE", pipe)
    monkeypatch.setattr(event_loop, "APIX_EVENT_LOOP", loop)
    monkeypatch.setattr(node_graph, "EVENT_PIPE", pipe)
    try:
        yield pipe, loop
    finally:
        await loop.stop()
        tasks = list(loop._dispatch_tasks | loop._background_handler_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pipe.clear()
        for name in tuple(APIX_HANDLER_REGISTRY.registry):
            unsubscribe(name)


@pytest.mark.parametrize("publication", ["post_event", "put", "put_nowait"])
async def test_saturated_handlers_can_publish_follow_up_events(runtime, publication):
    """All occupied permits can publish while an older event is waiting."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    started, completed = [], []

    @subscribe("parent.*", time_out=None)
    async def parent(event):
        started.append(event.event_name)
        if len(started) == 2:
            entered.set()
        await release.wait()
        name = event.event_name.replace("parent", "child")
        if publication == "post_event":
            await pipe.post_event(event_type=EventType.WORKFLOW, event_name=name)
        else:
            child = ApixEvent(name, EventType.WORKFLOW, name, None, 0)
            if publication == "put":
                await pipe.put(child)
            else:
                pipe.put_nowait(child)
        completed.append(event.event_name)

    @subscribe("child.*", "waiting")
    async def child(event):
        completed.append(event.event_name)

    for index in range(2):
        await pipe.post_event(event_type=EventType.WORKFLOW, event_name=f"parent.{index}")
    await asyncio.wait_for(entered.wait(), 1)
    await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
    release.set()
    await asyncio.wait_for(pipe.join(), 1)
    assert sorted(completed) == ["child.0", "child.1", "parent.0", "parent.1", "waiting"]


@pytest.mark.parametrize("queue_capacity", [1, 3])
async def test_queue_capacity_and_running_dispatch_limit_are_independent(runtime, monkeypatch, queue_capacity):
    """A full execution budget still allows the independently bounded buffer to fill."""
    pipe, loop = runtime
    # Vary buffering independently of the two configured dispatch permits.
    monkeypatch.setattr(loop, "_processing_queue", asyncio.Queue(maxsize=queue_capacity))
    transferred, handlers_entered = asyncio.Event(), asyncio.Event()
    handlers_allowed = asyncio.Event()
    gets = active = peak = 0
    completed = []
    original_get = pipe.get

    async def observe_get(*args, **kwargs):
        nonlocal gets
        event = await original_get(*args, **kwargs)
        gets += 1
        # Two executing, a full processing buffer, and one pending transfer.
        if gets == 2 + queue_capacity + 1:
            transferred.set()
        return event

    monkeypatch.setattr(pipe, "get", observe_get)

    @subscribe("work.*", time_out=None)
    async def work(event):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 2:
            handlers_entered.set()
        await handlers_allowed.wait()
        completed.append(event.event_name)
        active -= 1

    try:
        for index in range(8):
            await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
        await asyncio.wait_for(handlers_entered.wait(), 1)
        await asyncio.wait_for(transferred.wait(), 1)
        assert active == 2
        assert gets == 2 + queue_capacity + 1
        assert pipe.qsize() == 8 - gets
        assert loop._processing_queue.qsize() == queue_capacity
        handlers_allowed.set()
        await asyncio.wait_for(pipe.join(), 1)
        assert completed == [f"work.{index}" for index in range(8)]
        assert peak == 2
    finally:
        handlers_allowed.set()


async def test_join_tracks_event_until_handler_finishes(runtime):
    """An empty ready queue is not enough for join to complete."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()

    @subscribe("work", time_out=None)
    async def work(event):
        entered.set()
        await release.wait()

    await pipe.post_event(event_type=EventType.INFO, event_name="work")
    await asyncio.wait_for(entered.wait(), 1)
    assert pipe.empty()
    waiter = asyncio.create_task(pipe.join())
    try:
        await asyncio.sleep(0)
        assert not waiter.done()
        release.set()
        await asyncio.wait_for(waiter, 1)
    finally:
        release.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_publication_during_stop_keeps_restarted_consumers_alive(runtime):
    """A running handler may restart publication while old workers stop."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    @subscribe("parent", time_out=None)
    async def parent(event):
        entered.set()
        await release.wait()
        await pipe.post_event(event_type=EventType.INFO, event_name="child")

    @subscribe("child")
    async def child(event):
        completed.append(event.event_name)

    await pipe.post_event(event_type=EventType.INFO, event_name="parent")
    await asyncio.wait_for(entered.wait(), 1)
    stopping = asyncio.create_task(loop.stop())
    asyncio.get_running_loop().call_soon(release.set)
    await asyncio.wait_for(stopping, 1)
    await asyncio.wait_for(pipe.join(), 1)
    assert completed == ["child"]
    assert loop.started


async def test_task_creation_failure_does_not_block_later_events(runtime, monkeypatch):
    """A failed dispatch launch must acknowledge the event and free its permit."""
    pipe, loop = runtime
    create_task = asyncio.create_task
    failed = False
    completed = []

    def fail_first_dispatch(coroutine, **kwargs):
        nonlocal failed
        if coroutine.cr_code.co_name == "_dispatch_event" and not failed:
            failed = True
            raise RuntimeError("dispatch launch failed")
        return create_task(coroutine, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", fail_first_dispatch)

    @subscribe("work.*")
    async def work(event):
        completed.append(event.event_name)

    for index in range(4):
        await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
    await asyncio.wait_for(pipe.join(), 1)
    await asyncio.wait_for(loop.stop(), 1)
    assert failed
    assert completed == ["work.1", "work.2", "work.3"]


@pytest.mark.parametrize("restart_count", [1, 3])
async def test_stop_during_blocked_transfer_preserves_fifo_and_join(runtime, monkeypatch, restart_count):
    """Stopping put() on a full buffer must retain its event across restarts."""
    pipe, loop = runtime
    entered, release, blocked = asyncio.Event(), asyncio.Event(), asyncio.Event()
    started, completed = [], []
    original_put = loop._processing_queue.put

    async def observe_put(event):
        if event.event_name == "work.3":
            blocked.set()
        await original_put(event)

    monkeypatch.setattr(loop._processing_queue, "put", observe_put)

    @subscribe("work.*", time_out=None)
    async def work(event):
        started.append(event.event_name)
        if len(started) == 2:
            entered.set()
        await release.wait()
        completed.append(event.event_name)

    for index in range(6):
        await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.wait_for(blocked.wait(), 1)
    for index in range(restart_count):
        await asyncio.wait_for(loop.stop(), 1)
        assert not loop.started
        if index < restart_count - 1:
            blocked.clear()
            await loop.start()
            await asyncio.wait_for(blocked.wait(), 1)
    assert started == ["work.0", "work.1"]
    release.set()
    await asyncio.gather(*loop._dispatch_tasks)
    assert completed == ["work.0", "work.1"]
    waiter = asyncio.create_task(pipe.join())
    try:
        await asyncio.sleep(0)
        assert not waiter.done()
        await pipe.post_event(event_type=EventType.INFO, event_name="work.6")
        await asyncio.wait_for(waiter, 1)
        assert completed == [f"work.{index}" for index in range(7)]
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_concurrent_graphs_route_next_nodes_under_saturation(runtime):
    """Real graph nodes can post next hops while every permit is occupied."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    first_nodes = 0

    async def first(state):
        nonlocal first_nodes
        first_nodes += 1
        if first_nodes == 2:
            entered.set()
        await release.wait()
        return {"value": state["value"] + 1}

    def second(state):
        return {"value": state["value"] * 2}

    graph = (
        GraphManager().add_node(first).add_node(second)
        .add_edge(START, "first").add_edge("first", "second")
        .add_edge("second", END).compile_graph()
    )
    runs = [asyncio.create_task(graph.invoke({"value": value})) for value in (1, 5)]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
        release.set()
        assert await asyncio.wait_for(asyncio.gather(*runs), 1) == [
            {"value": 4}, {"value": 12},
        ]
        await asyncio.wait_for(pipe.join(), 1)
    finally:
        release.set()
        for run in runs:
            run.cancel()
        await asyncio.gather(*runs, return_exceptions=True)
        graph.decompose()

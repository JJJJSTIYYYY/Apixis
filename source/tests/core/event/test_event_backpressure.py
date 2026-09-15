"""Behaviour regressions for ready admission and processing backpressure."""

import asyncio

import pytest

from apixis.core.event import get_event_registry

from apixis.core.event import ApixEvent, EventType, subscribe, unsubscribe
from apixis.core.event import event_loop, event_pipe
from apixis.core.event.factory import get_handler_registry
from apixis.core.graph import START, END, GraphManager
from apixis.core.graph import node_graph


MIN_BACKPRESSURE = 128


@pytest.fixture
async def runtime(monkeypatch, request):
    """Exercise real publication with the production minimum backpressure."""
    for name in tuple(get_handler_registry().registry):
        unsubscribe(name)
    monkeypatch.setattr(event_loop, "EVENT_LOOP_BACKPRESSURE", getattr(request, "param", 2))
    monkeypatch.setattr(event_loop, "BACKGROUND_HANDLER_BACKPRESSURE", 1)
    monkeypatch.setattr(event_loop, "SHOW_EVENT_DISPATCH", False)
    pipe = event_pipe.ApixEventPipe(remote_enabled=False)
    loop = event_loop.ApixEventLoop(get_handler_registry(), pipe, get_event_registry())
    async def start_runtime():
        await pipe.start()
        await loop.start()

    monkeypatch.setattr(node_graph, "get_event_pipe", lambda: pipe)
    await start_runtime()
    try:
        yield pipe, loop
    finally:
        await loop.stop()
        tasks = list(loop._dispatch_tasks | loop._background_handler_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pipe.clear()
        for name in tuple(get_handler_registry().registry):
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
        if len(started) == MIN_BACKPRESSURE:
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

    for index in range(MIN_BACKPRESSURE):
        await pipe.post_event(event_type=EventType.WORKFLOW, event_name=f"parent.{index}")
    await asyncio.wait_for(entered.wait(), 1)
    # Fill the processing buffer while every dispatch permit is occupied.
    for _ in range(MIN_BACKPRESSURE):
        await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
    await asyncio.sleep(0)
    assert loop._processing_queue.full()
    release.set()
    await asyncio.wait_for(pipe.join(), 1)
    assert sorted(completed) == sorted(
        [f"{kind}.{index}" for kind in ("parent", "child") for index in range(MIN_BACKPRESSURE)]
        + ["waiting"] * MIN_BACKPRESSURE
    )


@pytest.mark.parametrize("runtime, capacity", [
    (-1, 128), (0, 128), (2, 128), (127, 128), (128, 128), (129, 129), (256, 256),
], indirect=["runtime"])
async def test_queue_capacity_and_running_dispatch_limit_are_independent(runtime, monkeypatch, capacity):
    """A full execution budget still allows the independently bounded buffer to fill."""
    pipe, loop = runtime
    event_count = 2 * capacity + 3
    transferred, handlers_entered = asyncio.Event(), asyncio.Event()
    handlers_allowed = asyncio.Event()
    gets = active = peak = 0
    completed = []
    await loop.stop()
    original_get = pipe.get

    async def observe_get(*args, **kwargs):
        nonlocal gets
        event = await original_get(*args, **kwargs)
        gets += 1
        # A full execution budget, a full buffer, and one pending transfer.
        if gets == 2 * capacity + 1:
            transferred.set()
        return event

    monkeypatch.setattr(pipe, "get", observe_get)
    await loop.start()

    @subscribe("work.*", time_out=None)
    async def work(event):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == capacity:
            handlers_entered.set()
        await handlers_allowed.wait()
        completed.append(event.event_name)
        active -= 1

    try:
        for index in range(event_count):
            await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
        await asyncio.wait_for(handlers_entered.wait(), 1)
        await asyncio.wait_for(transferred.wait(), 1)
        assert active == capacity
        assert gets == 2 * capacity + 1
        assert pipe.qsize() == event_count - gets
        assert loop._processing_queue.qsize() == capacity
        assert loop._processing_queue.full()
        handlers_allowed.set()
        await asyncio.wait_for(pipe.join(), 1)
        assert completed == [f"work.{index}" for index in range(event_count)]
        assert peak == capacity
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


async def test_explicit_start_during_stop_keeps_restarted_consumers_alive(runtime):
    """Explicit startup may resume consumption while old workers stop."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    @subscribe("parent", time_out=None)
    async def parent(event):
        entered.set()
        await release.wait()
        await loop.start()
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
    assert loop._started


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
    event_count = 2 * MIN_BACKPRESSURE + 3

    async def observe_put(event):
        if loop._processing_queue.full() and len(started) == MIN_BACKPRESSURE:
            blocked.set()
        await original_put(event)

    monkeypatch.setattr(loop._processing_queue, "put", observe_put)

    @subscribe("work.*", time_out=None)
    async def work(event):
        started.append(event.event_name)
        if len(started) == MIN_BACKPRESSURE:
            entered.set()
        await release.wait()
        completed.append(event.event_name)

    # Start all handlers before filling the buffer so the observed put blocks
    # on processing capacity rather than initial worker scheduling.
    for index in range(MIN_BACKPRESSURE):
        await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
    await asyncio.wait_for(entered.wait(), 1)
    for index in range(MIN_BACKPRESSURE, event_count):
        await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")
    await asyncio.wait_for(blocked.wait(), 1)
    for index in range(restart_count):
        await asyncio.wait_for(loop.stop(), 1)
        assert not loop._started
        if index < restart_count - 1:
            blocked.clear()
            await loop.start()
            await asyncio.wait_for(blocked.wait(), 1)
    assert started == [f"work.{index}" for index in range(MIN_BACKPRESSURE)]
    release.set()
    await asyncio.gather(*loop._dispatch_tasks)
    assert completed == [f"work.{index}" for index in range(MIN_BACKPRESSURE)]
    waiter = asyncio.create_task(pipe.join())
    try:
        await asyncio.sleep(0)
        assert not waiter.done()
        await loop.start()
        await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{event_count}")
        await asyncio.wait_for(waiter, 1)
        assert completed == [f"work.{index}" for index in range(event_count + 1)]
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
        if first_nodes == MIN_BACKPRESSURE:
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
    runs = [asyncio.create_task(graph.invoke({"value": value})) for value in range(MIN_BACKPRESSURE)]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        for _ in range(MIN_BACKPRESSURE):
            await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
        await asyncio.sleep(0)
        assert loop._processing_queue.full()
        release.set()
        assert await asyncio.wait_for(asyncio.gather(*runs), 1) == [
            {"value": (value + 1) * 2} for value in range(MIN_BACKPRESSURE)
        ]
        await asyncio.wait_for(pipe.join(), 1)
    finally:
        release.set()
        for run in runs:
            run.cancel()
        await asyncio.gather(*runs, return_exceptions=True)
        graph.decompose()

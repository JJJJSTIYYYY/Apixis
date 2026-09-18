"""Behaviour regressions for bounded publication and foreground event backpressure."""

import asyncio

import pytest

from apixis.core.event import ApixEvent, EventType, subscribe, unsubscribe
from apixis.core.event import event_loop, event_pipe, factory
from apixis.core.event.factory import get_handler_registry
from apixis.core.graph import START, END, GraphManager


EVENT_CAPACITY = 2


@pytest.fixture
async def runtime(monkeypatch, request):
    """Exercise real publication with small, independently configured limits."""
    for name in tuple(get_handler_registry().registry):
        unsubscribe(name)
    monkeypatch.setattr(event_loop, "EVENT_LOOP_BACKPRESSURE", getattr(request, "param", 2))
    monkeypatch.setattr(event_pipe, "EVENT_PIPE_MAX_LEN", 2)
    monkeypatch.setattr(event_loop, "SHOW_EVENT_DISPATCH", False)
    # Inject one complete core so sync and async getters share the test runtime.
    registry = factory.ApixEventRegistry()
    pipe = event_pipe.ApixEventPipe(remote_enabled=False)
    handlers = factory.ApixHandlerRegistry(registry)
    loop = event_loop.ApixEventLoop(handlers, pipe, registry)
    core = factory.EventCore(registry, pipe, handlers, loop)
    monkeypatch.setattr(factory, "_core", core)
    await factory.start_core(core)
    try:
        yield pipe, loop
    finally:
        await loop.stop()
        tasks = list(loop._dispatch_tasks | loop._background_handler_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pipe.clear()
        await pipe.stop()
        for name in tuple(handlers.registry):
            handlers.unregister_handler(name)


async def test_saturated_handlers_can_publish_follow_up_events(runtime, wait_for_dispatch):
    """A full queue and all occupied permits must still allow handler posts."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    started, completed = [], []

    @subscribe("parent.*", time_out=None)
    async def parent(event):
        started.append(event.event_name)
        if len(started) == EVENT_CAPACITY:
            entered.set()
        await release.wait()
        for index in range(4):
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name=f"child.{event.event_name}.{index}",
            )
        completed.append(event.event_name)

    @subscribe("child.*", "waiting")
    async def child(event):
        completed.append(event.event_name)

    for index in range(EVENT_CAPACITY):
        await pipe.post_event(event_type=EventType.WORKFLOW, event_name=f"parent.{index}")
    await asyncio.wait_for(entered.wait(), 1)
    # One event waits for a permit; the remaining events fill the only queue.
    await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
    await asyncio.sleep(0)
    for _ in range(pipe.maxsize):
        pipe.put_nowait(ApixEvent("id", EventType.INFO, "waiting", None, 0))
    assert pipe.full()
    release.set()
    await wait_for_dispatch(loop)
    assert len(completed) == EVENT_CAPACITY * 5 + pipe.maxsize + 1


@pytest.mark.parametrize("runtime", [1], indirect=True)
async def test_event_permit_is_held_across_handlers_in_the_same_chain(runtime, wait_for_dispatch):
    """One event retains its permit across its entire foreground handler chain."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    @subscribe("work.*", priority=2)
    async def first(event):
        calls.append((event.event_name, "first"))
        if event.event_name == "work.a":
            entered.set()
            await release.wait()

    @subscribe("work.*", priority=1)
    async def second(event):
        calls.append((event.event_name, "second"))

    await pipe.post_event(event_type=EventType.INFO, event_name="work.a")
    await asyncio.wait_for(entered.wait(), 1)
    await pipe.post_event(event_type=EventType.INFO, event_name="work.b")
    await asyncio.sleep(0)
    release.set()
    await wait_for_dispatch(loop)
    assert calls == [
        ("work.a", "first"), ("work.a", "second"),
        ("work.b", "first"), ("work.b", "second"),
    ]
    for name in ("work.a", "work.b"):
        assert [phase for event, phase in calls if event == name] == ["first", "second"]


async def test_join_acknowledges_dispatch_without_waiting_for_handlers(runtime):
    """A dispatched event is acknowledged while its foreground handler runs."""
    pipe, loop = runtime
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    @subscribe("work", time_out=None)
    async def work(event):
        entered.set()
        await release.wait()
        finished.set()

    try:
        await pipe.post_event(event_type=EventType.INFO, event_name="work")
        await asyncio.wait_for(entered.wait(), 1)
        assert pipe.empty()
        await asyncio.wait_for(pipe.join(), 1)
        assert not finished.is_set()
        release.set()
        await asyncio.wait_for(finished.wait(), 1)
    finally:
        release.set()


async def test_explicit_start_during_stop_keeps_restarted_consumers_alive(runtime, wait_for_dispatch):
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
    await wait_for_dispatch(loop)
    assert completed == ["child"]
    assert loop._started


async def test_task_creation_failure_does_not_block_later_events(runtime, monkeypatch, wait_for_dispatch):
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
    await wait_for_dispatch(loop)
    await asyncio.wait_for(loop.stop(), 1)
    assert failed
    assert completed == ["work.1", "work.2", "work.3"]


async def test_concurrent_graphs_route_next_nodes_under_saturation(runtime, wait_for_dispatch):
    """Real graph nodes can post next hops while every permit is occupied."""
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    first_nodes = 0

    async def first(state):
        nonlocal first_nodes
        first_nodes += 1
        if first_nodes == EVENT_CAPACITY:
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
    runs = [asyncio.create_task(graph.invoke({"value": value})) for value in range(EVENT_CAPACITY)]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        for _ in range(EVENT_CAPACITY):
            await pipe.post_event(event_type=EventType.INFO, event_name="waiting")
        await asyncio.sleep(0)
        assert pipe.qsize() <= pipe.maxsize
        release.set()
        assert await asyncio.wait_for(asyncio.gather(*runs), 1) == [
            {"value": (value + 1) * 2} for value in range(EVENT_CAPACITY)
        ]
        await wait_for_dispatch(loop)
    finally:
        release.set()
        for run in runs:
            run.cancel()
        await asyncio.gather(*runs, return_exceptions=True)
        graph.decompose()


@pytest.mark.parametrize("runtime", [1], indirect=True)
async def test_idle_consumer_does_not_block_the_next_handler(runtime, wait_for_dispatch):
    """Waiting for the next event must leave capacity for an existing chain."""
    pipe, loop = runtime
    calls = []

    @subscribe("work", priority=2)
    async def first(event):
        await asyncio.sleep(0)
        calls.append("first")

    @subscribe("work", priority=1)
    async def second(event):
        calls.append("second")

    await pipe.post_event(event_type=EventType.INFO, event_name="work")
    await wait_for_dispatch(loop)
    assert calls == ["first", "second"]


@pytest.mark.parametrize("runtime", [1, 2, 4], indirect=True)
async def test_event_saturation_applies_backpressure_to_external_producers(runtime, request):
    """Occupied event slots leave one pending event and a bounded queue."""
    pipe, loop = runtime
    capacity = request.node.callspec.params["runtime"]
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    active = peak = completed = 0
    count = capacity + 8

    @subscribe("work.*")
    async def work(event):
        nonlocal active, peak, completed
        active += 1
        peak = max(peak, active)
        if active == capacity:
            entered.set()
        await release.wait()
        completed += 1
        active -= 1
        if completed == count:
            finished.set()

    async def produce():
        for index in range(count):
            await pipe.post_event(event_type=EventType.INFO, event_name=f"work.{index}")

    producer = asyncio.create_task(produce())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        # Wait until both the pending slot and the local queue are occupied.
        async with asyncio.timeout(1):
            while loop._pending_event is None or not pipe.full():
                await asyncio.sleep(0)
        assert not producer.done()
        assert active == capacity
        assert completed == 0
        assert factory.get_event_registry().get_registered_events() == frozenset(
            f"work.{index}" for index in range(capacity)
        )
        release.set()
        await asyncio.wait_for(producer, 1)
        await asyncio.wait_for(finished.wait(), 1)
        assert peak == capacity
        assert completed == count
    finally:
        release.set()
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)


@pytest.mark.parametrize("publication", ["post_event", "put"])
async def test_bounded_queue_blocks_external_producers_until_consumed(runtime, publication):
    """The only queue enforces its configured capacity through native put()."""
    pipe, loop = runtime
    await loop.stop()
    event = ApixEvent("id", EventType.INFO, "queued", None, 0)
    for _ in range(pipe.maxsize):
        pipe.put_nowait(event)
    with pytest.raises(asyncio.QueueFull):
        pipe.put_nowait(event)
    if publication == "post_event":
        producer = asyncio.create_task(pipe.post_event(event_type=EventType.INFO, event_name="queued"))
    else:
        producer = asyncio.create_task(pipe.put(event))
    try:
        await asyncio.sleep(0)
        assert pipe.maxsize == 2
        assert pipe.full()
        assert not producer.done()
        await loop.start()
        await asyncio.wait_for(producer, 1)
        await asyncio.wait_for(pipe.join(), 1)
    finally:
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)


@pytest.mark.parametrize("runtime", [1], indirect=True)
@pytest.mark.parametrize("child_task", [False, True])
@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
async def test_post_uses_inherited_handler_context_and_restores_slot(runtime, monkeypatch, child_task, outcome, wait_for_dispatch):
    """Direct calls and inherited tasks release the same handler slot during put."""
    pipe, loop = runtime
    publishing, blocker_entered, finish_put = asyncio.Event(), asyncio.Event(), asyncio.Event()
    release_blocker, resumed = asyncio.Event(), asyncio.Event()
    original_put = pipe.put
    posting_task = None

    async def controlled_put(event, *args, **kwargs):
        if event.event_name == "child":
            publishing.set()
            await finish_put.wait()
            if outcome == "error":
                raise ValueError("publication failed")
            return
        await original_put(event, *args, **kwargs)

    monkeypatch.setattr(pipe, "put", controlled_put)

    async def publish():
        nonlocal posting_task
        posting_task = asyncio.current_task()
        await pipe.post_event(event_type=EventType.INFO, event_name="child")

    @subscribe("parent")
    async def parent(event):
        try:
            if child_task:
                await asyncio.create_task(publish())
            else:
                await publish()
        except (ValueError, asyncio.CancelledError):
            pass
        finally:
            resumed.set()

    @subscribe("blocker")
    async def blocker(event):
        blocker_entered.set()
        await release_blocker.wait()

    try:
        await pipe.post_event(event_type=EventType.INFO, event_name="parent")
        await asyncio.wait_for(publishing.wait(), 1)
        await pipe.post_event(event_type=EventType.INFO, event_name="blocker")
        await asyncio.wait_for(blocker_entered.wait(), 1)
        if outcome == "cancel":
            posting_task.cancel()
        else:
            finish_put.set()
        await asyncio.sleep(0)
        assert not resumed.is_set()
        release_blocker.set()
        await wait_for_dispatch(loop)
        assert resumed.is_set()

        active = peak = 0

        @subscribe("probe")
        async def probe(event):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1

        for _ in range(6):
            await pipe.post_event(event_type=EventType.INFO, event_name="probe")
        await wait_for_dispatch(loop)
        assert peak == 1
    finally:
        finish_put.set()
        release_blocker.set()


@pytest.mark.parametrize("runtime", [1], indirect=True)
async def test_background_handlers_overlap_after_event_capacity_is_available(runtime):
    """Background events wait for dispatch, then their handlers run independently."""
    pipe, loop = runtime
    foreground_entered, all_background_entered = asyncio.Event(), asyncio.Event()
    release_foreground, release_background = asyncio.Event(), asyncio.Event()
    finished = asyncio.Event()
    active = peak = completed = 0

    @subscribe("foreground")
    async def foreground(event):
        foreground_entered.set()
        await release_foreground.wait()

    @subscribe("background", background=True)
    async def background(event):
        nonlocal active, peak, completed
        active += 1
        peak = max(peak, active)
        if active == 4:
            all_background_entered.set()
        await release_background.wait()
        active -= 1
        completed += 1
        if completed == 4:
            finished.set()

    async def produce():
        for _ in range(4):
            await pipe.post_event(event_type=EventType.INFO, event_name="background")

    producer = None
    try:
        await pipe.post_event(event_type=EventType.INFO, event_name="foreground")
        await asyncio.wait_for(foreground_entered.wait(), 1)
        producer = asyncio.create_task(produce())
        async with asyncio.timeout(1):
            while loop._pending_event is None or not pipe.full():
                await asyncio.sleep(0)
        assert active == 0
        assert not producer.done()
        release_foreground.set()
        await asyncio.wait_for(producer, 1)
        await asyncio.wait_for(all_background_entered.wait(), 1)
        # A single event slot does not impose a background execution limit.
        assert active == peak == 4
        await asyncio.wait_for(pipe.join(), 1)
        assert completed == 0
        release_background.set()
        await asyncio.wait_for(finished.wait(), 1)
        assert completed == 4
    finally:
        release_foreground.set()
        release_background.set()
        if producer is not None:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)


async def test_stop_and_restart_preserve_queued_events(runtime, wait_for_dispatch):
    """Stop pauses dequeueing and restart consumes each queued event once."""
    pipe, loop = runtime
    completed = []

    @subscribe("queued.*")
    async def receive(event):
        completed.append(event.event_name)

    await loop.stop()
    for index in range(pipe.maxsize):
        await pipe.post_event(event_type=EventType.INFO, event_name=f"queued.{index}")
    await loop.stop()
    assert pipe.full()
    assert completed == []
    await loop.start()
    await wait_for_dispatch(loop)
    assert completed == [f"queued.{index}" for index in range(pipe.maxsize)]

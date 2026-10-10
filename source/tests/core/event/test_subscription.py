"""Tests for event-waiting and temporary process suspension helpers."""

import asyncio

import pytest

from apixis.core.event.base import (
    ApixEvent,
    EventType,
    suspend_process,
)
from apixis.core.event.factory import (
    get_event_loop,
    get_event_pipe,
    get_handler_registry,
)
from apixis.core.event.subscription import (
    EventWaiter,
    await_for,
    subscribe,
    unsubscribe,
    wait_for_event,
)


@pytest.fixture(autouse=True)
def isolate_handlers():
    """Keep the process-global handler registry isolated for these tests."""
    registry = get_handler_registry()
    for name in tuple(registry.registry):
        registry.unregister_handler(name)
    yield
    for name in tuple(registry.registry):
        registry.unregister_handler(name)


async def wait_until_registered(previous_names: set[str]) -> None:
    """Wait until await_for has installed its transient handler."""
    registry = get_handler_registry()
    async with asyncio.timeout(1):
        while set(registry.registry) == previous_names:
            await asyncio.sleep(0)


async def test_suspend_process_without_handler_context_is_a_noop():
    async with suspend_process():
        await asyncio.sleep(0)


async def test_await_for_received_resolves_before_foreground_processing(
    wait_for_dispatch,
):
    pipe = get_event_pipe()
    loop = get_event_loop()
    entered = asyncio.Event()
    release = asyncio.Event()

    @subscribe("job.*", priority=9999)
    async def process(event):
        entered.set()
        await release.wait()
        event.context["processed"] = True

    registered = set(get_handler_registry().registry)
    waiter = asyncio.create_task(await_for("job.*", point="received"))
    await wait_until_registered(registered)
    try:
        await pipe.post_event(
            event_type=EventType.INFO,
            event_name="job.created",
            context={"processed": False},
        )
        result = await asyncio.wait_for(waiter, 1)
        assert isinstance(result, ApixEvent)
        assert result.event_name == "job.created"
        assert entered.is_set()
        assert result.context == {"processed": False}
        assert set(get_handler_registry().registry) == registered
    finally:
        release.set()
        await wait_for_dispatch(loop)
        unsubscribe(process.__name__)


async def test_await_for_processed_resolves_after_foreground_processing(
    wait_for_dispatch,
):
    pipe = get_event_pipe()
    loop = get_event_loop()
    entered = asyncio.Event()
    release = asyncio.Event()

    @subscribe("job.*", priority=-9999)
    async def process(event):
        entered.set()
        await release.wait()
        event.context["processed"] = True

    registered = set(get_handler_registry().registry)
    waiter = asyncio.create_task(await_for("job.*", point="processed"))
    await wait_until_registered(registered)
    try:
        await pipe.post_event(
            event_type=EventType.INFO,
            event_name="job.created",
            context={"processed": False},
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert not waiter.done()
        release.set()
        result = await asyncio.wait_for(waiter, 1)
        assert isinstance(result, ApixEvent)
        assert result.context == {"processed": True}
        assert set(get_handler_registry().registry) == registered
    finally:
        release.set()
        await wait_for_dispatch(loop)
        unsubscribe(process.__name__)


@pytest.mark.parametrize("upstream_state", ["accepted", "error"])
async def test_await_for_processed_resolves_for_terminal_event_state(
    upstream_state,
    wait_for_dispatch,
):
    pipe = get_event_pipe()
    loop = get_event_loop()

    @subscribe("job.*")
    async def process(event):
        if upstream_state == "accepted":
            event.accept()
        else:
            raise ValueError("processing failed")

    registered = set(get_handler_registry().registry)
    waiter = asyncio.create_task(await_for("job.*", point="processed"))
    await wait_until_registered(registered)
    try:
        await pipe.post_event(event_type=EventType.INFO, event_name="job.created")
        result = await asyncio.wait_for(waiter, 1)
        assert result.accepted is (upstream_state == "accepted")
        assert result.has_error is (upstream_state == "error")
    finally:
        await wait_for_dispatch(loop)
        unsubscribe(process.__name__)


async def test_await_for_ignores_filtered_events(wait_for_dispatch):
    pipe = get_event_pipe()
    loop = get_event_loop()
    registered = set(get_handler_registry().registry)
    waiter = asyncio.create_task(
        await_for("job.*", filter=["job.ignore.*"], time_out=1)
    )
    await wait_until_registered(registered)
    try:
        await pipe.post_event(
            event_type=EventType.INFO,
            event_name="job.ignore.internal",
        )
        await wait_for_dispatch(loop)
        assert not waiter.done()

        await pipe.post_event(event_type=EventType.INFO, event_name="job.created")
        result = await waiter
        assert result.event_name == "job.created"
        assert set(get_handler_registry().registry) == registered
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await wait_for_dispatch(loop)


async def test_await_for_timeout_removes_transient_handler():
    registry = get_handler_registry()
    registered = set(registry.registry)

    with pytest.raises(TimeoutError):
        await await_for("job.never", time_out=0.01)

    assert set(registry.registry) == registered


async def test_await_for_cancellation_removes_transient_handler():
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = asyncio.create_task(await_for("job.never"))
    await wait_until_registered(registered)

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert set(registry.registry) == registered


async def test_wait_for_event_registers_on_entry_and_retains_first_reply(
    wait_for_dispatch,
):
    """Publish immediately on entry, then retrieve a reply that already arrived."""
    registry = get_handler_registry()
    registered = set(registry.registry)
    context = wait_for_event("job.*", point="processed", time_out=1)
    assert set(registry.registry) == registered

    async with context as waiter:
        assert isinstance(waiter, EventWaiter)
        assert len(set(registry.registry) - registered) == 1
        for result in ("first", "second"):
            await get_event_pipe().post_event(
                event_type=EventType.INFO,
                event_name="job.finished",
                context={"result": result},
            )
        # Both events are fully dispatched before wait() starts.
        await wait_for_dispatch(get_event_loop())
        event = await waiter.wait()
        assert isinstance(event, ApixEvent)
        assert event.context == {"result": "first"}
        assert await waiter.wait() is event

    assert set(registry.registry) == registered
    with pytest.raises(RuntimeError, match="exited"):
        await waiter.wait()


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_wait_for_event_observes_selected_foreground_point(
    point, wait_for_dispatch,
):
    entered, release = asyncio.Event(), asyncio.Event()

    @subscribe("job.finished")
    async def process(event):
        entered.set()
        await release.wait()
        event.context["processed"] = True

    task = None
    try:
        async with wait_for_event("job.finished", point=point, time_out=1) as waiter:
            await get_event_pipe().post_event(
                event_type=EventType.INFO,
                event_name="job.finished",
                context={"processed": False},
            )
            await asyncio.wait_for(entered.wait(), 1)
            if point == "received":
                event = await waiter.wait()
                assert event.context == {"processed": False}
            else:
                task = asyncio.create_task(waiter.wait())
                await asyncio.sleep(0)
                assert not task.done()
                release.set()
                event = await task
                assert event.context == {"processed": True}
    finally:
        release.set()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await wait_for_dispatch(get_event_loop())


@pytest.mark.parametrize("upstream_state", ["accepted", "error", "cancelled"])
async def test_wait_for_event_processed_observes_terminal_events(
    upstream_state, wait_for_dispatch,
):
    @subscribe("job.finished")
    async def process(event):
        if upstream_state == "accepted":
            event.accept()
        elif upstream_state == "cancelled":
            raise asyncio.CancelledError
        else:
            raise ValueError("processing failed")

    async with wait_for_event("job.finished", point="processed", time_out=1) as waiter:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.finished",
        )
        event = await waiter.wait()
        assert event.accepted is (upstream_state == "accepted")
        assert event.has_error is (upstream_state in {"error", "cancelled"})
    await wait_for_dispatch(get_event_loop())


@pytest.mark.parametrize("filter", ["job.ignore.*", ["job.ignore.*"]])
async def test_wait_for_event_filters_events(filter, wait_for_dispatch):
    async with wait_for_event("job.*", filter=filter, time_out=1) as waiter:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.ignore.internal",
        )
        await wait_for_dispatch(get_event_loop())
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.finished",
        )
        assert (await waiter.wait()).event_name == "job.finished"


async def test_wait_for_event_deadline_includes_work_before_wait():
    async with wait_for_event("job.never", time_out=0.01) as waiter:
        # The deadline expires the result without interrupting the body.
        await asyncio.sleep(0.02)
        with pytest.raises(TimeoutError):
            await waiter.wait()


async def test_wait_for_event_retains_timely_result_after_deadline(wait_for_dispatch):
    async with wait_for_event("job.finished", time_out=0.1) as waiter:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.finished",
        )
        await wait_for_dispatch(get_event_loop())
        await asyncio.sleep(0.11)
        assert (await waiter.wait()).event_name == "job.finished"


async def test_wait_for_event_rejects_reply_after_deadline(wait_for_dispatch):
    async with wait_for_event("job.finished", time_out=0.01) as waiter:
        await asyncio.sleep(0.02)
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.finished",
        )
        await wait_for_dispatch(get_event_loop())
        with pytest.raises(TimeoutError):
            await waiter.wait()


@pytest.mark.parametrize("time_out", [0, -1])
async def test_wait_for_event_nonpositive_timeout_expires_immediately(time_out):
    async with wait_for_event("job.never", time_out=time_out) as waiter:
        with pytest.raises(TimeoutError):
            await waiter.wait()


@pytest.mark.parametrize("exit_mode", ["unused", "error", "timeout", "cancelled"])
async def test_wait_for_event_exit_always_removes_subscription(exit_mode):
    registry = get_handler_registry()
    registered = set(registry.registry)

    async def run():
        async with wait_for_event("job.never", time_out=0.01) as waiter:
            if exit_mode == "error":
                raise ValueError("publication failed")
            if exit_mode == "timeout":
                await waiter.wait()
            if exit_mode == "cancelled":
                asyncio.current_task().cancel()
                await waiter.wait()

    if exit_mode == "error":
        with pytest.raises(ValueError, match="publication failed"):
            await run()
    elif exit_mode == "timeout":
        with pytest.raises(TimeoutError):
            await run()
    elif exit_mode == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await asyncio.create_task(run())
    else:
        await run()
    assert set(registry.registry) == registered


async def test_wait_for_event_unobserved_timeout_does_not_report_future_error():
    loop = asyncio.get_running_loop()
    errors = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context))
    try:
        async with wait_for_event("job.never", time_out=0.01):
            await asyncio.sleep(0.02)
        await asyncio.sleep(0)
        assert errors == []
    finally:
        loop.set_exception_handler(previous_handler)


async def test_wait_for_event_does_not_replay_previously_consumed_events(
    wait_for_dispatch,
):
    await get_event_pipe().post_event(
        event_type=EventType.INFO, event_name="job.finished",
    )
    await wait_for_dispatch(get_event_loop())
    async with wait_for_event("job.finished", time_out=0.01) as waiter:
        with pytest.raises(TimeoutError):
            await waiter.wait()


@pytest.mark.parametrize("event_runtime_capacity", [1], indirect=True)
async def test_wait_for_event_request_reply_lends_saturated_handler_permit(
    event_runtime_capacity, wait_for_dispatch,
):
    """A handler can register, publish and await a fast reply with one slot."""
    pipe = get_event_pipe()
    done = asyncio.Event()
    results = []

    @subscribe("tool.request")
    async def tool(event):
        await pipe.post_event(
            event_type=EventType.WORKFLOW,
            event_name=event.context["reply"],
            context={"result": "ok"},
        )

    @subscribe("tool.execute")
    async def execute(event):
        async with wait_for_event("tool.reply", point="processed", time_out=1) as waiter:
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name="tool.request",
                context={"reply": "tool.reply"},
            )
            results.append((await waiter.wait()).context["result"])
        done.set()

    await pipe.post_event(event_type=EventType.WORKFLOW, event_name="tool.execute")
    await asyncio.wait_for(done.wait(), 1)
    await wait_for_dispatch(get_event_loop())
    assert results == ["ok"]

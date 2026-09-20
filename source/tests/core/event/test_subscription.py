"""Tests for event-waiting and temporary process suspension helpers."""

import asyncio

import pytest

from apixis.core.event.base import (
    ApixEvent,
    EventType,
    handler_semaphore_context,
    suspend_process,
)
from apixis.core.event.factory import (
    get_event_loop,
    get_event_pipe,
    get_handler_registry,
)
from apixis.core.event.subscription import await_for, subscribe, unsubscribe


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


async def test_suspend_process_releases_and_restores_handler_capacity():
    semaphore = asyncio.BoundedSemaphore(1)
    await semaphore.acquire()
    token = handler_semaphore_context.set(semaphore)
    try:
        assert semaphore.locked()
        async with suspend_process():
            assert not semaphore.locked()
        assert semaphore.locked()
    finally:
        handler_semaphore_context.reset(token)
        semaphore.release()


async def test_suspend_process_restores_capacity_after_body_failure():
    semaphore = asyncio.BoundedSemaphore(1)
    await semaphore.acquire()
    token = handler_semaphore_context.set(semaphore)
    try:
        with pytest.raises(RuntimeError, match="failed while suspended"):
            async with suspend_process():
                assert not semaphore.locked()
                raise RuntimeError("failed while suspended")
        assert semaphore.locked()
    finally:
        handler_semaphore_context.reset(token)
        semaphore.release()


async def test_nested_suspend_process_releases_capacity_only_once():
    semaphore = asyncio.BoundedSemaphore(1)
    await semaphore.acquire()
    token = handler_semaphore_context.set(semaphore)
    try:
        async with suspend_process():
            assert not semaphore.locked()
            async with suspend_process():
                assert not semaphore.locked()
            assert not semaphore.locked()
        assert semaphore.locked()
    finally:
        handler_semaphore_context.reset(token)
        semaphore.release()


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
        await_for("job.*", filter="job.ignore.*", time_out=1)
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

"""Cancellation notifications terminate dispatch without resuming business work."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from apixis.core.event import ApixEvent, ApixEventHandler, EventType
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.handler_registry import ApixHandlerRegistry


@pytest.fixture(autouse=True)
def isolate_registry():
    """Keep the process-wide registry independent between contract tests."""
    registry = ApixHandlerRegistry()
    for name in list(registry.registry):
        registry.unregister_handler(name)
    yield
    for name in list(registry.registry):
        registry.unregister_handler(name)


def register(registry, name, core_func, **kwargs):
    """Register a handler through the public registry contract."""
    handler = ApixEventHandler(core_func, **kwargs)
    handler.name = name
    handler.subscribe = ["cancel.test"]
    handler.priority = 1
    registry.register_handler(handler)
    return handler


def make_event():
    return ApixEvent("cancel-id", EventType.WORKFLOW, "cancel.test", None, 0)


@pytest.mark.parametrize("phase", ["core_func", "on_has_error", "on_accepted", "on_error"])
@pytest.mark.parametrize("stop_when_error", [False, True])
async def test_cancelled_phase_notifies_entire_foreground_chain(phase, stop_when_error):
    """Completed, current and unreached subscribers all receive cleanup once."""
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    event = make_event()
    notified = []

    def cleanup(name):
        async def on_cancelled(event):
            notified.append(name)
        return on_cancelled

    async def prepare(event):
        if phase == "on_accepted":
            event.accept()
        elif phase == "on_has_error":
            raise ValueError("upstream failure")

    async def cancel(*args):
        raise asyncio.CancelledError("original cancellation")

    register(registry, "before", prepare, on_cancelled=cleanup("before"))
    callbacks = {"core_func": AsyncMock(), phase: cancel}
    if phase == "on_error":
        callbacks["core_func"] = AsyncMock(side_effect=ValueError("core failure"))
    register(
        registry, "current", on_cancelled=cleanup("current"),
        stop_when_error=stop_when_error, **callbacks,
    )
    tail_core = AsyncMock()
    register(registry, "after", tail_core, on_cancelled=cleanup("after"),
             stop_when_error=False)
    background_cleanup = AsyncMock()
    register(registry, "background", AsyncMock(), background=True,
             on_cancelled=background_cleanup)

    task = asyncio.create_task(runtime._dispatch_event(
        event, registry.get_handlers_chain_for_event(event.event_name),
    ))
    with pytest.raises(asyncio.CancelledError, match="original cancellation"):
        await asyncio.wait_for(task, 1)

    assert task.cancelled()
    assert sorted(notified) == ["after", "before", "current"]
    tail_core.assert_not_awaited()
    background_cleanup.assert_not_awaited()
    assert [error.exception_type for error in event.error_stack] == (
        ["ValueError"] if phase in ("on_error", "on_has_error") else []
    )


@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_cleanup_failure_cannot_hide_cancellation_or_skip_other_subscribers(failure):
    """A failing cleanup preserves the original cancellation and other hooks."""
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    event = make_event()
    original = asyncio.CancelledError("original cancellation")

    async def cancel(event):
        raise original

    async def broken_cleanup(event):
        if failure == "error":
            raise ValueError("cleanup failure")
        if failure == "cancel":
            raise asyncio.CancelledError("cleanup cancelled")
        await asyncio.Future()

    on_error = AsyncMock()
    register(registry, "broken", cancel, on_cancelled=broken_cleanup,
             on_error=on_error, time_out=0.01 if failure == "timeout" else None)
    tail_cleanup = AsyncMock()
    register(registry, "tail", AsyncMock(), on_cancelled=tail_cleanup)
    with patch("apixis.core.event.base.logger") as logger:
        task = asyncio.create_task(runtime._dispatch_event(event, ["broken", "tail"]))
        with pytest.raises(asyncio.CancelledError, match="original cancellation"):
            await asyncio.wait_for(task, 1)
        logger.error.assert_called_once()
    tail_cleanup.assert_awaited_once_with(event)
    on_error.assert_not_awaited()
    assert event.error_stack == []


async def test_cancel_notifications_run_concurrently_and_wait_for_every_hook():
    """Every cleanup starts while the others wait, and dispatch waits for all."""
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    event = make_event()
    entered = [asyncio.Event() for _ in range(3)]
    release = [asyncio.Event() for _ in range(3)]
    finished = [asyncio.Event() for _ in range(3)]
    completed = []
    tail_core = AsyncMock()

    async def cancel(event):
        raise asyncio.CancelledError("original cancellation")

    def cleanup(index):
        async def on_cancelled(event):
            entered[index].set()
            await release[index].wait()
            completed.append(index)
            finished[index].set()
        return on_cancelled

    for index, core in enumerate((AsyncMock(), cancel, tail_core)):
        register(registry, f"handler.{index}", core,
                 on_cancelled=cleanup(index), time_out=None)

    task = asyncio.create_task(runtime._dispatch_event(
        event, registry.get_handlers_chain_for_event(event.event_name),
    ))
    try:
        # A serial implementation cannot reach all three barriers.
        await asyncio.wait_for(asyncio.gather(*(signal.wait() for signal in entered)), 1)
        assert not task.done()
        for index in (2, 0):
            release[index].set()
            await asyncio.wait_for(finished[index].wait(), 1)
            assert not task.done()
        release[1].set()
        with pytest.raises(asyncio.CancelledError, match="original cancellation"):
            await asyncio.wait_for(task, 1)
        assert completed == [2, 0, 1]
        tail_core.assert_not_awaited()
    finally:
        for signal in release:
            signal.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_repeated_dispatch_cancellation_interrupts_all_running_notifications():
    """Cancelling gather interrupts its hooks and propagates the new cancellation."""
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    event = make_event()
    entered = [asyncio.Event(), asyncio.Event()]
    interrupted = []
    tail_core = AsyncMock()
    on_error = AsyncMock()

    async def cancel(event):
        raise asyncio.CancelledError("original cancellation")

    def cleanup(index):
        async def on_cancelled(event):
            entered[index].set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                interrupted.append(index)
                raise
        return on_cancelled

    register(registry, "first", cancel, on_cancelled=cleanup(0),
             on_error=on_error, time_out=None)
    register(registry, "tail", tail_core, on_cancelled=cleanup(1),
             on_error=on_error, time_out=None)
    with patch("apixis.core.event.base.logger") as logger:
        task = asyncio.create_task(runtime._dispatch_event(event, ["first", "tail"]))
        try:
            await asyncio.wait_for(asyncio.gather(*(signal.wait() for signal in entered)), 1)
            task.cancel("second cancellation")
            # gather propagates cancellation without preserving its message.
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
            assert task.cancelled()
            assert sorted(interrupted) == [0, 1]
            assert logger.error.call_count == 2
            tail_core.assert_not_awaited()
            on_error.assert_not_awaited()
            assert event.error_stack == []
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_external_dispatch_cancellation_notifies_after_core_cleanup():
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    entered = asyncio.Event()
    calls = []

    async def core(event):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            calls.append("core cleanup")

    async def cleanup(event):
        calls.append("cancel notification")

    register(registry, "waiting", core, on_cancelled=cleanup)
    task = asyncio.create_task(runtime._dispatch_event(make_event(), ["waiting"]))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert calls == ["core cleanup", "cancel notification"]


async def test_cancellation_while_waiting_for_background_capacity_notifies_foreground():
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    runtime._background_handler_semaphore = asyncio.Semaphore(1)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def background(event):
        entered.set()
        await release.wait()

    background_cleanup = AsyncMock()
    register(registry, "background", background, background=True,
             on_cancelled=background_cleanup)
    foreground_core = AsyncMock()
    foreground_cleanup = AsyncMock()
    register(registry, "foreground", foreground_core, on_cancelled=foreground_cleanup)
    event = make_event()
    await runtime._create_background_handler_task("background", event)
    await asyncio.wait_for(entered.wait(), 1)
    task = asyncio.create_task(runtime._dispatch_event(event, ["background", "foreground"]))
    try:
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        foreground_cleanup.assert_awaited_once_with(event)
        foreground_core.assert_not_awaited()
        background_cleanup.assert_not_awaited()
    finally:
        release.set()
        await asyncio.gather(*runtime._background_handler_tasks)


@pytest.mark.parametrize("cause", ["raise", "external"])
async def test_background_cancellation_only_notifies_its_own_handler(cause):
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)
    entered = asyncio.Event()

    async def core(event):
        entered.set()
        if cause == "raise":
            raise asyncio.CancelledError()
        await asyncio.Future()

    background_cleanup = AsyncMock()
    foreground_cleanup = AsyncMock()
    register(registry, "background", core, background=True, on_cancelled=background_cleanup)
    register(registry, "foreground", AsyncMock(), on_cancelled=foreground_cleanup)
    event = make_event()
    task = asyncio.create_task(runtime._run_background_handler("background", event))
    await asyncio.wait_for(entered.wait(), 1)
    if cause == "external":
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    background_cleanup.assert_awaited_once_with(event)
    foreground_cleanup.assert_not_awaited()
    assert event.error_stack == []


@pytest.mark.parametrize("action", ["success", "accept", "error", "timeout"])
async def test_non_cancellation_does_not_notify_cancelled(action):
    registry = ApixHandlerRegistry()
    runtime = ApixEventLoop(registry)

    async def core(event):
        if action == "accept":
            event.accept()
        elif action == "error":
            raise ValueError("ordinary failure")
        elif action == "timeout":
            await asyncio.Future()

    cleanup = AsyncMock()
    register(registry, "work", core, on_cancelled=cleanup, time_out=0.01)
    await runtime._dispatch_event(make_event(), ["work"])
    cleanup.assert_not_awaited()


def test_on_cancelled_registration_validation_and_replacement():
    with pytest.raises(TypeError):
        ApixEventHandler(AsyncMock(), on_cancelled=1)
    registry = ApixHandlerRegistry()
    cleanup = AsyncMock()
    handler = register(registry, "handler", AsyncMock(), on_cancelled=cleanup)
    assert registry.get_handler("handler").on_cancelled is cleanup
    with pytest.raises(ValueError, match="on_cancelled"):
        handler.add_on_cancelled_callback(AsyncMock(), exist_ok=False)
    with pytest.raises(TypeError):
        handler.add_on_cancelled_callback(1)
    replacement = AsyncMock()
    handler.add_on_cancelled_callback(replacement)
    assert handler.on_cancelled is replacement
    handler.on_cancelled = 1
    with pytest.raises(TypeError, match="on_cancelled"):
        registry.register_handler(handler, exist_ok=True)

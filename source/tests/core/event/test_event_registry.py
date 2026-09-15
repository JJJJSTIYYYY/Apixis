"""Tests for the registry of exact event names observed at runtime."""

import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

import apixis.core.event as event_package
from apixis.core.event import ApixEventLoop, ApixHandlerRegistry, ApixEventHandler, BuiltinChannel
from apixis.core.event.base import ApixEvent, EventType
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.factory import get_event_registry


def make_event(event_name: str) -> ApixEvent:
    """Create one event for registry tests."""
    return ApixEvent(
        event_id=f"event-{event_name}",
        event_type=EventType.WORKFLOW,
        event_name=event_name,
        context=None,
        timestamp=time.time(),
    )


def test_event_registry_class_and_factory_getter_are_exported():
    assert event_package.ApixEventRegistry is ApixEventRegistry
    assert event_package.get_event_registry() is get_event_registry()
    assert ApixEventRegistry() is not get_event_registry()


def test_record_event_stores_exact_case_sensitive_names_once():
    get_event_registry().record_event(make_event("Graph.Start"))
    get_event_registry().record_event(make_event("Graph.Start"))
    get_event_registry().record_event(make_event("graph.start"))

    assert get_event_registry().get_registered_events() == frozenset(
        {"Graph.Start", "graph.start"}
    )


def test_get_registered_events_returns_immutable_snapshot():
    get_event_registry().record_event(make_event("event.one"))
    snapshot = get_event_registry().get_registered_events()
    get_event_registry().record_event(make_event("event.two"))

    assert snapshot == frozenset({"event.one"})
    assert get_event_registry().get_registered_events() == frozenset(
        {"event.one", "event.two"}
    )


def test_clear_forgets_observed_names():
    get_event_registry().record_event(make_event("event.one"))

    get_event_registry().clear()

    assert get_event_registry().get_registered_events() == frozenset()


def test_record_event_validates_input():
    with pytest.raises(TypeError, match="ApixEvent"):
        get_event_registry().record_event(object())
    with pytest.raises(ValueError, match="event_name"):
        get_event_registry().record_event(make_event(""))


@pytest.mark.asyncio
async def test_post_event_only_buffers_before_dispatch():
    pipe = ApixEventPipe(remote_enabled=False)

    await pipe.post_event(
        event_type=EventType.INFO,
        event_name="runtime.posted",
    )

    assert get_event_registry().get_registered_events() == frozenset()
    await pipe.get()
    pipe.task_done()


def test_put_nowait_only_buffers_events_and_other_queue_values():
    pipe = ApixEventPipe(remote_enabled=False)

    pipe.put_nowait(make_event("runtime.nowait"))
    pipe.put_nowait("raw-value")

    assert get_event_registry().get_registered_events() == frozenset()
    assert pipe.get_nowait().event_name == "runtime.nowait"
    pipe.task_done()
    assert pipe.get_nowait() == "raw-value"
    pipe.task_done()


@pytest.mark.asyncio
async def test_mailtruck_publish_does_not_record_local_observations():
    mailtruck = AsyncMock()
    pipe = ApixEventPipe(
        remote_enabled=False,
        mailtruck=mailtruck,
    )
    event = make_event("runtime.remote")

    await pipe.put(event, "mailtruck", recipient="node-two")

    mailtruck.put.assert_awaited_once_with(event, recipient="node-two")
    assert get_event_registry().get_registered_events() == frozenset()


@pytest.mark.asyncio
async def test_failed_publish_does_not_record_event_name():
    mailtruck = AsyncMock()
    mailtruck.put.side_effect = RuntimeError("publish failed")
    pipe = ApixEventPipe(
        remote_enabled=False,
        mailtruck=mailtruck,
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        await pipe.put(
            make_event("runtime.failed"),
            "mailtruck",
            recipient="node-two",
        )

    assert get_event_registry().get_registered_events() == frozenset()


@pytest.mark.parametrize("publication", ["post_event", "put", "put_nowait", "builtin", "mailbox"])
@pytest.mark.parametrize("event_type, name", [
    (EventType.INFO, "runtime.observed"),
    (EventType.INTERNAL, "runtime.internal"),
    (EventType.INFO, ""),
])
async def test_recording_occurs_once_at_processing_dequeue(publication, event_type, name, monkeypatch):
    """Ready and processing buffers are unobserved until dispatch capacity exists."""
    registry = ApixEventRegistry()
    handlers = ApixHandlerRegistry(registry)
    mailbox = BuiltinChannel()
    gateway = AsyncMock()
    gateway.broadcast.return_value = {}
    gateway.fetch_nodes.return_value = {}
    pipe = ApixEventPipe(
        mailbox=mailbox, mailtruck=gateway, remote_enabled=publication == "mailbox",
    )
    loop = ApixEventLoop(handlers, pipe, registry)
    capacity = asyncio.Semaphore(0)
    loop._dispatch_semaphore = capacity
    record = Mock(wraps=registry.record_event)
    monkeypatch.setattr(registry, "record_event", record)
    observed_in_handler = []

    async def receive(event):
        observed_in_handler.append(registry.get_registered_events())

    handler = ApixEventHandler(receive)
    handler.subscribe = ["runtime.*"]
    handler.priority = 1
    handlers.register_handler(handler)
    event = make_event(name)
    event.event_type = event_type
    await pipe.start()
    try:
        if publication == "post_event":
            await pipe.post_event(event_type=event_type, event_name=name)
        elif publication == "put":
            await pipe.put(event)
        elif publication == "put_nowait":
            pipe.put_nowait(event)
        elif publication == "builtin":
            await pipe.get_channel("builtin").put(event)
        else:
            await mailbox.put(event)
            await asyncio.wait_for(mailbox.join(), 1)
        assert registry.get_registered_events() == frozenset()
        record.assert_not_called()
        await loop.start()
        # Let admission finish while dispatch remains blocked by capacity.
        await asyncio.sleep(0)
        assert pipe.empty()
        assert loop._processing_queue.qsize() == 1
        record.assert_not_called()
        capacity.release()
        await asyncio.wait_for(pipe.join(), 1)
        expected = frozenset({name}) if name and event_type != EventType.INTERNAL else frozenset()
        assert registry.get_registered_events() == expected
        assert record.call_count == (1 if name else 0)
        assert observed_in_handler == ([expected] if name else [])
    finally:
        await loop.stop()
        await pipe.stop()


async def test_broadcast_does_not_record_local_observations():
    registry = get_event_registry()
    gateway = AsyncMock()
    gateway.broadcast.return_value = {}
    pipe = ApixEventPipe(mailtruck=gateway, remote_enabled=True)
    event = make_event("runtime.broadcast")
    await pipe.broadcast(event)
    gateway.broadcast.assert_awaited_once_with(event)
    assert registry.get_registered_events() == frozenset()

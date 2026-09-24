"""Cancellation and restart contracts through public pipe and channel APIs."""

import asyncio
from types import SimpleNamespace

import pytest

from apixis import ApixEvent, ApixEventPipe, BuiltinChannel, EventType, WritableEventChannel


def event(name):
    return ApixEvent(name, EventType.INFO, name, None, 0)


class TrackedBuffer(BuiltinChannel):
    """A real queue with observable lifecycle boundaries and cleanup gates."""

    def __init__(self, maxsize):
        super().__init__(maxsize=maxsize)
        self.taken = asyncio.Queue()
        self.reading = asyncio.Event()
        self.reader_cancelled = asyncio.Event()
        self.allow_reader_exit = asyncio.Event()
        self.allow_reader_exit.set()
        self.close_entered = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.allow_close.set()
        self.reader = None
        self.started = 0
        self.closed = 0
        self.acknowledged = 0

    async def start(self):
        self.started += 1

    async def get(self):
        self.reader = asyncio.current_task()
        self.reading.set()
        try:
            value = await super().get()
        except asyncio.CancelledError:
            self.reader_cancelled.set()
            await self.allow_reader_exit.wait()
            raise
        self.taken.put_nowait(value)
        return value

    def task_done(self):
        super().task_done()
        self.acknowledged += 1

    async def close(self):
        self.close_entered.set()
        await self.allow_close.wait()
        self.closed += 1


class TrackedGateway(WritableEventChannel):
    """Control online, offline and connection-close waits without network I/O."""

    def __init__(self):
        self.online_entered = asyncio.Event()
        self.allow_online = asyncio.Event()
        self.allow_online.set()
        self.offline_entered = asyncio.Event()
        self.allow_offline = asyncio.Event()
        self.allow_offline.set()
        self.close_entered = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.allow_close.set()
        self.started = 0
        self.closed = 0
        self.broadcasts = []

    async def start(self):
        self.started += 1

    async def put(self, event, **kwargs):
        pass

    async def broadcast(self, event):
        self.broadcasts.append(event.event_name)
        if event.event_name == "apixis.node.online":
            self.online_entered.set()
            await self.allow_online.wait()
        else:
            self.offline_entered.set()
            await self.allow_offline.wait()
        return {}

    async def close(self):
        self.close_entered.set()
        await self.allow_close.wait()
        self.closed += 1


@pytest.fixture
async def remote_pipe():
    builtin, mailbox, gateway = TrackedBuffer(1), TrackedBuffer(3), TrackedGateway()
    pipe = ApixEventPipe(
        builtin=builtin, mailbox=mailbox, mailtruck=gateway, remote_enabled=True,
    )
    tasks = []

    def spawn(coroutine):
        task = asyncio.create_task(coroutine)
        tasks.append(task)
        return task

    yield SimpleNamespace(
        pipe=pipe, builtin=builtin, mailbox=mailbox, gateway=gateway, spawn=spawn,
    )

    # Release test gates before waiting for cancellation-safe cleanup.
    builtin.allow_close.set()
    mailbox.allow_close.set()
    mailbox.allow_reader_exit.set()
    gateway.allow_close.set()
    gateway.allow_online.set()
    gateway.allow_offline.set()
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pipe.stop()


@pytest.mark.parametrize("pauses", [1, 3])
async def test_pending_mailbox_event_survives_repeated_stop_and_restart(remote_pipe, pauses):
    r = remote_pipe
    local, first, second = event("local"), event("remote.first"), event("remote.second")
    await r.pipe.put(local)
    await r.mailbox.put(first)
    await r.mailbox.put(second)
    await r.pipe.start()
    assert await asyncio.wait_for(r.mailbox.taken.get(), 1) is first
    joined = r.spawn(r.pipe.join("mailbox"))

    for index in range(pauses):
        # Shutdown must finish even though no builtin capacity is available.
        await asyncio.wait_for(r.pipe.stop(), 1)
        assert r.mailbox.acknowledged == 0
        assert not joined.done()
        assert r.mailbox.qsize() == 1
        if index + 1 < pauses:
            await r.pipe.start()

    assert r.pipe.get_nowait() is local
    r.pipe.task_done()
    await r.pipe.start()
    for expected in (first, second):
        assert await asyncio.wait_for(r.pipe.get(), 1) is expected
        r.pipe.task_done()
    await asyncio.wait_for(joined, 1)
    await asyncio.wait_for(r.pipe.join(), 1)
    assert r.mailbox.acknowledged == 2
    assert r.pipe.empty()
    assert r.mailbox.empty()


async def test_successful_handoff_is_not_repeated_after_restart(remote_pipe):
    r = remote_pipe
    message = event("remote.delivered")
    await r.pipe.start()
    await r.mailbox.put(message)
    await asyncio.wait_for(r.mailbox.join(), 1)
    await r.pipe.stop()
    await r.pipe.start()
    await r.pipe.stop()
    assert r.pipe.qsize() == 1
    assert r.pipe.get_nowait() is message
    r.pipe.task_done()
    assert r.mailbox.acknowledged == 1


@pytest.mark.parametrize("phase", ["broadcast", "forwarder", "mailbox", "mailtruck", "builtin"])
async def test_cancelled_stop_finishes_all_cleanup_before_propagating(remote_pipe, phase):
    r = remote_pipe
    await r.pipe.start()
    await asyncio.wait_for(r.mailbox.reading.wait(), 1)
    reader = r.mailbox.reader
    if phase == "broadcast":
        r.gateway.allow_offline.clear()
        entered = r.gateway.offline_entered
    elif phase == "forwarder":
        r.mailbox.allow_reader_exit.clear()
        entered = r.mailbox.reader_cancelled
    else:
        channel = r.pipe.get_channel(phase)
        channel.allow_close.clear()
        entered = channel.close_entered
    # Keep cleanup pending long enough to test a repeated cancellation.
    r.builtin.allow_close.clear()
    stopped = r.spawn(r.pipe.stop())
    await asyncio.wait_for(entered.wait(), 1)
    stopped.cancel("first cancellation")
    await asyncio.sleep(0)
    stopped.cancel("repeated cancellation")
    await asyncio.sleep(0)
    assert not stopped.done()
    r.mailbox.allow_reader_exit.set()
    r.mailbox.allow_close.set()
    r.gateway.allow_close.set()
    r.builtin.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(stopped, 1)
    assert reader.done()
    assert [r.builtin.closed, r.mailbox.closed, r.gateway.closed] == [1, 1, 1]

    # A later stop is safe and does not repeat completed cleanup.
    await asyncio.wait_for(r.pipe.stop(), 1)
    assert [r.builtin.closed, r.mailbox.closed, r.gateway.closed] == [1, 1, 1]
    r.gateway.allow_offline.set()
    await r.pipe.start()
    message = event("remote.after-cancelled-stop")
    await r.mailbox.put(message)
    assert await asyncio.wait_for(r.pipe.get(), 1) is message
    r.pipe.task_done()
    await asyncio.wait_for(r.mailbox.join(), 1)


async def test_start_and_second_stop_wait_for_cancelled_shutdown(remote_pipe):
    r = remote_pipe
    await r.pipe.start()
    r.mailbox.allow_close.clear()
    stopped = r.spawn(r.pipe.stop())
    await asyncio.wait_for(r.mailbox.close_entered.wait(), 1)
    stopped.cancel()
    second_stop = r.spawn(r.pipe.stop())
    restarted = r.spawn(r.pipe.start())
    await asyncio.sleep(0)
    assert not second_stop.done()
    assert not restarted.done()
    assert [r.builtin.started, r.mailbox.started, r.gateway.started] == [1, 1, 1]

    r.mailbox.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(stopped, 1)
    await asyncio.wait_for(second_stop, 1)
    await asyncio.wait_for(restarted, 1)
    assert [r.builtin.closed, r.mailbox.closed, r.gateway.closed] == [1, 1, 1]
    assert [r.builtin.started, r.mailbox.started, r.gateway.started] == [2, 2, 2]
    assert r.gateway.broadcasts.count("apixis.node.offline") == 1
    message = event("remote.after-concurrent-restart")
    await r.mailbox.put(message)
    assert await asyncio.wait_for(r.pipe.get(), 1) is message
    r.pipe.task_done()


async def test_cancelled_start_cleans_up_and_preserves_pending_mailbox_event(remote_pipe):
    r = remote_pipe
    r.gateway.allow_online.clear()
    local, pending = event("local"), event("remote.pending-during-start")
    await r.pipe.put(local)
    await r.mailbox.put(pending)
    starting = r.spawn(r.pipe.start())
    await asyncio.wait_for(r.mailbox.taken.get(), 1)
    r.mailbox.allow_close.clear()
    starting.cancel()
    await asyncio.wait_for(r.mailbox.close_entered.wait(), 1)
    starting.cancel()
    await asyncio.sleep(0)
    assert not starting.done()
    r.mailbox.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(starting, 1)
    assert [r.builtin.closed, r.mailbox.closed, r.gateway.closed] == [1, 1, 1]
    assert r.mailbox.acknowledged == 0
    assert r.pipe.get_nowait() is local
    r.pipe.task_done()
    r.gateway.allow_online.set()
    await r.pipe.start()
    assert await asyncio.wait_for(r.pipe.get(), 1) is pending
    r.pipe.task_done()
    await asyncio.wait_for(r.mailbox.join(), 1)
    assert r.mailbox.acknowledged == 1

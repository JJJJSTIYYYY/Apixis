"""Broker admission, restart and cancellation through public channel/pipe APIs."""

import asyncio
from types import SimpleNamespace
import sys

import pytest
from aiokafka import TopicPartition
from aio_pika.message import ProcessContext

from apixis import (
    ApixEvent, ApixEventHandler, ApixEventLoop, ApixEventPipe, ApixEventRegistry,
    ApixHandlerRegistry, BuiltinChannel, EventCore, EventType, KafkaChannel,
    RabbitMQChannel, WritableEventChannel, get_event_pipe, start_core,
)
from apixis.core.event.pipe_channel import encode_event
from apixis.core.utils.exception import EventChannelUnavailableError


EOF = object()


def event(name):
    return ApixEvent(name, EventType.INFO, name, None, 0)


async def eventually(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


async def receive(channel):
    result = await asyncio.wait_for(channel.get(), 2)
    channel.task_done()
    return result


class KafkaBroker:
    """Persist records/offsets separately from replaceable consumer sessions."""

    def __init__(self):
        self.records = []
        self.committed = {}
        self.commits = []
        self.consumers = []
        self.delivered = asyncio.Queue()
        self.committing = asyncio.Queue()
        self.opened = asyncio.Event()
        self.allow_open = asyncio.Event()
        self.allow_open.set()
        self.closing = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.allow_close.set()
        self.allow_commit = asyncio.Event()
        self.allow_commit.set()
        self.commit_error = None

    def feed(self, name, *, partition=0, payload=None):
        offset = sum(r.partition == partition for r in self.records)
        record = SimpleNamespace(
            topic="mailbox.node", partition=partition, offset=offset,
            value=encode_event(event(name)) if payload is None else payload,
        )
        self.records.append(record)
        for consumer in self.consumers:
            if consumer.started and not consumer.stopped:
                consumer.inbox.put_nowait(record)
        return record

    def consumer(self, *topics, **options):
        owner = self

        class Consumer:
            def __init__(self):
                self.inbox = asyncio.Queue()
                self.started = False
                self.stopped = False

            async def start(self):
                owner.opened.set()
                await owner.allow_open.wait()
                self.started = True
                for record in owner.records:
                    tp = TopicPartition(record.topic, record.partition)
                    fallback = 0 if options["auto_offset_reset"] == "earliest" else len(owner.records)
                    if record.offset >= owner.committed.get(tp, fallback):
                        self.inbox.put_nowait(record)

            def __aiter__(self):
                return self

            async def __anext__(self):
                item = await self.inbox.get()
                if item is EOF:
                    raise StopAsyncIteration
                if isinstance(item, BaseException):
                    raise item
                owner.delivered.put_nowait(item)
                return item

            async def commit(self, offsets):
                owner.committing.put_nowait(offsets)
                await owner.allow_commit.wait()
                if owner.commit_error is not None:
                    raise owner.commit_error
                owner.committed.update(offsets)
                owner.commits.append(dict(offsets))

            async def stop(self):
                owner.closing.set()
                await owner.allow_close.wait()
                # The real SDK must not perform a final fetched-position commit.
                assert options["enable_auto_commit"] is False
                self.stopped = True

        consumer = Consumer()
        self.consumers.append(consumer)
        return consumer


@pytest.fixture
async def kafka(monkeypatch):
    broker = KafkaBroker()
    monkeypatch.setitem(sys.modules, "aiokafka", SimpleNamespace(
        AIOKafkaConsumer=broker.consumer, TopicPartition=TopicPartition,
    ))
    channel = KafkaChannel(
        mq_id="node", bootstrap_servers="broker", topic_prefix="mailbox",
        group_id_prefix="nodes", maxsize=1,
    )
    yield broker, channel
    broker.allow_open.set()
    broker.allow_close.set()
    broker.allow_commit.set()
    await channel.close()


async def test_kafka_full_buffer_stop_replays_only_unadmitted_record(kafka):
    broker, channel = kafka
    broker.feed("first")
    broker.feed("second")
    await channel.start()
    await asyncio.wait_for(broker.delivered.get(), 2)
    await asyncio.wait_for(broker.delivered.get(), 2)
    await channel.close()
    assert broker.committed == {TopicPartition("mailbox.node", 0): 1}
    assert (await receive(channel)).event_name == "first"
    await channel.start()
    assert (await receive(channel)).event_name == "second"
    await eventually(lambda: broker.committed[TopicPartition("mailbox.node", 0)] == 2)
    assert channel.empty()


async def test_kafka_first_record_without_a_commit_is_not_skipped(kafka):
    broker, channel = kafka
    # Fill the real channel before opening a fresh consumer group.
    # This setup stands for preserved local events from a previous session.
    broker.feed("preserved")
    await channel.start()
    await eventually(lambda: bool(broker.commits))
    await channel.close()
    broker.committed.clear()
    broker.records.clear()
    broker.feed("first-uncommitted")
    while not broker.delivered.empty():
        broker.delivered.get_nowait()
    await channel.start()
    await asyncio.wait_for(broker.delivered.get(), 2)
    await channel.close()
    assert broker.committed == {}
    assert (await receive(channel)).event_name == "preserved"
    await channel.start()
    assert (await receive(channel)).event_name == "first-uncommitted"


async def test_kafka_commits_exact_partition_offsets_and_skips_invalid_payload(kafka):
    broker, channel = kafka
    broker.feed("p0", partition=0)
    broker.feed("invalid", partition=1, payload=b"not-json")
    broker.feed("p1", partition=1)
    await channel.start()
    assert (await receive(channel)).event_name == "p0"
    assert (await receive(channel)).event_name == "p1"
    await eventually(lambda: len(broker.commits) == 3)
    assert broker.commits == [
        {TopicPartition("mailbox.node", 0): 1},
        {TopicPartition("mailbox.node", 1): 1},
        {TopicPartition("mailbox.node", 1): 2},
    ]


async def test_kafka_commit_failure_preserves_local_copy_and_allows_redelivery(kafka):
    broker, channel = kafka
    broker.feed("admitted")
    broker.commit_error = RuntimeError("coordinator unavailable")
    await channel.start()
    await eventually(lambda: not channel.is_running)
    assert (await receive(channel)).event_name == "admitted"
    assert not broker.committed
    broker.commit_error = None
    await channel.start()
    assert broker.consumers[0].stopped
    assert (await receive(channel)).event_name == "admitted"
    await eventually(lambda: bool(broker.committed))


async def test_kafka_cancel_during_commit_keeps_record_recoverable(kafka):
    broker, channel = kafka
    broker.feed("admitted")
    broker.allow_commit.clear()
    await channel.start()
    await asyncio.wait_for(broker.committing.get(), 2)
    await asyncio.wait_for(channel.close(), 2)
    assert not broker.committed
    assert (await receive(channel)).event_name == "admitted"
    broker.allow_commit.set()
    await channel.start()
    assert (await receive(channel)).event_name == "admitted"


class RabbitBroker:
    """Use real aio-pika acknowledgement policy with a durable in-memory queue."""

    def __init__(self):
        self.pending = asyncio.Queue()
        self.delivered = asyncio.Queue()
        self.connections = []
        self.acked = []
        self.setting_up = asyncio.Event()
        self.allow_setup = asyncio.Event()
        self.allow_setup.set()
        self.rejected = []
        self.opened = asyncio.Event()
        self.allow_open = asyncio.Event()
        self.allow_open.set()
        self.closing = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.allow_close.set()

    def feed(self, name, *, payload=None):
        self.pending.put_nowait((name, encode_event(event(name)) if payload is None else payload))

    async def connect(self, url):
        self.opened.set()
        await self.allow_open.wait()
        owner = self

        class Connection:
            def __init__(self):
                self.closed = False
                self.unacked = []
                self.readers = 0

            async def channel(self):
                owner.setting_up.set()
                await owner.allow_setup.wait()
                return self

            async def set_qos(self, **kwargs):
                pass

            async def declare_exchange(self, *args, **kwargs):
                return object()

            async def declare_queue(self, *args, **kwargs):
                return self

            async def bind(self, *args, **kwargs):
                pass

            def iterator(self):
                return self

            async def __aenter__(self):
                self.readers += 1
                return self

            async def __aexit__(self, *args):
                self.readers -= 1

            def __aiter__(self):
                return self

            async def __anext__(self):
                item = await owner.pending.get()
                if item is EOF:
                    raise StopAsyncIteration
                if isinstance(item, BaseException):
                    raise item
                name, payload = item
                connection = self

                class Message:
                    body = payload
                    processed = False
                    redelivered = False
                    channel = SimpleNamespace(is_closed=False)

                    def process(self, **kwargs):
                        return ProcessContext(self, requeue=kwargs.get("requeue", False),
                                              reject_on_redelivered=False, ignore_processed=False)

                    async def ack(self):
                        self.processed = True
                        owner.acked.append(name)

                    async def reject(self, *, requeue):
                        self.processed = True
                        owner.rejected.append((name, requeue))
                        if requeue:
                            owner.pending.put_nowait((name, payload))

                message = Message()
                connection.unacked.append((item, message))
                owner.delivered.put_nowait(name)
                return message

            async def close(self):
                owner.closing.set()
                await owner.allow_close.wait()
                self.closed = True
                for item, message in self.unacked:
                    if not message.processed:
                        owner.pending.put_nowait(item)
                        message.processed = True

        connection = Connection()
        self.connections.append(connection)
        return connection


@pytest.fixture
async def rabbit(monkeypatch):
    broker = RabbitBroker()
    monkeypatch.setitem(sys.modules, "aio_pika", SimpleNamespace(
        connect_robust=broker.connect, ExchangeType=SimpleNamespace(DIRECT="direct"),
    ))
    channel = RabbitMQChannel(
        mq_id="node", url="amqp://broker/", exchange="events",
        queue_prefix="mailbox", prefetch_count=2, maxsize=1,
    )
    yield broker, channel
    broker.allow_open.set()
    broker.allow_close.set()
    broker.allow_setup.set()
    await channel.close()


@pytest.fixture(params=["kafka", "rabbit"])
def transport(request):
    return request.param, request.getfixturevalue(request.param)


async def test_rabbit_full_buffer_shutdown_requeues_and_restart_receives(rabbit):
    broker, channel = rabbit
    broker.feed("first")
    broker.feed("second")
    await channel.start()
    assert await asyncio.wait_for(broker.delivered.get(), 2) == "first"
    assert await asyncio.wait_for(broker.delivered.get(), 2) == "second"
    await asyncio.wait_for(channel.close(), 2)
    assert broker.acked == ["first"]
    assert broker.rejected == [("second", True)]
    assert (await receive(channel)).event_name == "first"
    await channel.start()
    assert (await receive(channel)).event_name == "second"
    await eventually(lambda: broker.acked == ["first", "second"])
    assert broker.pending.empty()


async def test_rabbit_invalid_payload_is_acknowledged_without_requeue(rabbit):
    broker, channel = rabbit
    broker.feed("invalid", payload=b"not-json")
    broker.feed("valid")
    await channel.start()
    assert (await receive(channel)).event_name == "valid"
    assert broker.acked == ["invalid", "valid"]
    assert broker.rejected == []


async def test_parallel_start_and_close_own_one_generation(transport):
    kind, (broker, channel) = transport
    broker.allow_open.clear()
    starts = [asyncio.create_task(channel.start()) for _ in range(8)]
    await asyncio.wait_for(broker.opened.wait(), 2)
    closing = asyncio.create_task(channel.close())
    broker.allow_open.set()
    await asyncio.wait_for(asyncio.gather(*starts, closing), 2)
    sessions = broker.consumers if kind == "kafka" else broker.connections
    assert len(sessions) == 1
    assert sessions[0].stopped if kind == "kafka" else sessions[0].closed
    assert not channel.is_running
    if kind == "rabbit":
        assert sessions[0].readers == 0
    await channel.start()
    assert channel.is_running and len(sessions) == 2


async def test_repeated_cancellation_waits_for_close_before_restart(transport):
    kind, (broker, channel) = transport
    await channel.start()
    broker.allow_close.clear()
    closing = asyncio.create_task(channel.close())
    await asyncio.wait_for(broker.closing.wait(), 2)
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    restarting = asyncio.create_task(channel.start())
    await asyncio.sleep(0)
    assert not closing.done() and not restarting.done()
    broker.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(closing, 2)
    await asyncio.wait_for(restarting, 2)
    sessions = broker.consumers if kind == "kafka" else broker.connections
    assert len(sessions) == 2
    assert sessions[0].stopped if kind == "kafka" else sessions[0].closed
    assert channel.is_running


@pytest.mark.parametrize("exit_mode", ["error", "cancel", "eof"])
async def test_direct_start_replaces_exited_consumer(transport, exit_mode):
    kind, (broker, channel) = transport
    await channel.start()
    incoming = broker.consumers[-1].inbox if kind == "kafka" else broker.pending
    failure = {"error": RuntimeError("receiver failed"), "cancel": asyncio.CancelledError(), "eof": EOF}[exit_mode]
    incoming.put_nowait(failure)
    await eventually(lambda: not channel.is_running)
    await channel.start()
    broker.feed("after-restart")
    assert (await receive(channel)).event_name == "after-restart"
    sessions = broker.consumers if kind == "kafka" else broker.connections
    assert sessions[0].stopped if kind == "kafka" else sessions[0].closed


class Gateway(WritableEventChannel):
    def __init__(self):
        self.actions = []
        self.closed = 0
        self.fetch_error = None
        self.online_error = None
        self.offline_error = None
        self.online_entered = asyncio.Event()
        self.allow_online = asyncio.Event()
        self.allow_online.set()
        self.offline_entered = asyncio.Event()
        self.allow_offline = asyncio.Event()
        self.allow_offline.set()

    async def start(self):
        self.actions.append("start")

    async def put(self, message, **kwargs):
        pass

    async def fetch_nodes(self):
        self.actions.append("fetch")
        if self.fetch_error:
            raise self.fetch_error
        return {}

    async def broadcast(self, message):
        online = message.event_name.endswith(".online")
        self.actions.append("online" if online else "offline")
        if online:
            self.online_entered.set()
            await self.allow_online.wait()
            if self.online_error:
                raise self.online_error
        else:
            self.offline_entered.set()
            await self.allow_offline.wait()
            if self.offline_error:
                raise self.offline_error
        return {}

    async def close(self):
        self.closed += 1


async def test_core_getter_recovers_dead_mailbox(transport):
    kind, (broker, mailbox) = transport
    gateway = Gateway()
    pipe = ApixEventPipe(mailbox=mailbox, mailtruck=gateway, remote_enabled=True)
    registry = ApixEventRegistry()
    handlers = ApixHandlerRegistry(registry)
    loop = ApixEventLoop(handlers, pipe, registry)
    core = EventCore(registry, pipe, handlers, loop)
    received = asyncio.Event()
    async def callback(message):
        received.set()
    handler = ApixEventHandler(callback)
    handler.subscribe = ["after-core-restart"]
    handler.priority = 1
    handlers.register_handler(handler)
    try:
        await start_core(core)
        incoming = broker.consumers[-1].inbox if kind == "kafka" else broker.pending
        incoming.put_nowait(RuntimeError("receiver failed"))
        await eventually(lambda: not mailbox.is_running)
        assert not pipe.is_running and not core.started
        assert get_event_pipe(core) is pipe
        await eventually(lambda: core.started)
        broker.feed("after-core-restart")
        await asyncio.wait_for(received.wait(), 2)
        assert gateway.actions.count("online") == 2
    finally:
        if core.start_task:
            await core.start_task
        await loop.stop()
        await pipe.stop()


async def test_pipe_recovers_failed_forwarder_and_retains_pending_message():
    class FailingBuffer(BuiltinChannel):
        async def put(self, message, **kwargs):
            if not hasattr(self, "failed"):
                self.failed = True
                raise RuntimeError("local handoff failed")
            await super().put(message)
    mailbox = BuiltinChannel(2)
    pipe = ApixEventPipe(builtin=FailingBuffer(2), mailbox=mailbox,
                         mailtruck=Gateway(), remote_enabled=True)
    try:
        await pipe.start()
        await mailbox.put(event("pending"))
        await eventually(lambda: not pipe.is_running)
        await pipe.start()
        assert (await receive(pipe)).event_name == "pending"
        await asyncio.wait_for(mailbox.join(), 2)
    finally:
        await pipe.stop()


@pytest.mark.parametrize("stage", ["fetch", "online"])
async def test_startup_failure_never_leaves_an_uncompensated_online_attempt(stage):
    gateway = Gateway()
    error = RuntimeError("startup failed")
    setattr(gateway, f"{stage}_error", error)
    pipe = ApixEventPipe(mailbox=BuiltinChannel(1), mailtruck=gateway, remote_enabled=True)
    with pytest.raises(RuntimeError) as raised:
        await pipe.start()
    assert raised.value is error
    assert gateway.actions == (["start", "fetch"] if stage == "fetch" else ["start", "fetch", "online", "offline"])
    assert gateway.closed == 1
    assert not pipe.is_running
    setattr(gateway, f"{stage}_error", None)
    await pipe.start()
    assert pipe.is_running
    await pipe.stop()


async def test_cancelled_online_attempt_compensates_before_concurrent_restart():
    gateway = Gateway()
    gateway.allow_online.clear()
    gateway.allow_offline.clear()
    pipe = ApixEventPipe(mailbox=BuiltinChannel(1), mailtruck=gateway, remote_enabled=True)
    starting = asyncio.create_task(pipe.start())
    await asyncio.wait_for(gateway.online_entered.wait(), 2)
    starting.cancel()
    await asyncio.wait_for(gateway.offline_entered.wait(), 2)
    starting.cancel()
    restarting = asyncio.create_task(pipe.start())
    await asyncio.sleep(0)
    assert not starting.done() and not restarting.done()
    gateway.allow_online.set()
    gateway.allow_offline.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(starting, 2)
    await asyncio.wait_for(restarting, 2)
    assert gateway.actions == ["start", "fetch", "online", "offline", "start", "fetch", "online"]
    assert gateway.closed == 1
    await pipe.stop()


async def test_failed_compensation_still_closes_channels_and_preserves_original_error():
    gateway = Gateway()
    original = RuntimeError("online reply lost")
    gateway.online_error = original
    gateway.offline_error = ValueError("gateway offline")
    pipe = ApixEventPipe(mailbox=BuiltinChannel(1), mailtruck=gateway, remote_enabled=True)
    with pytest.raises(RuntimeError) as raised:
        await pipe.start()
    assert raised.value is original
    assert gateway.closed == 1
    assert gateway.actions[-1] == "offline"
    assert not pipe.is_running


async def test_cancelled_partial_start_finishes_cleanup_before_retry(transport):
    kind, (broker, channel) = transport
    gate = broker.allow_open if kind == "kafka" else broker.allow_setup
    entered = broker.opened if kind == "kafka" else broker.setting_up
    gate.clear()
    broker.allow_close.clear()
    starting = asyncio.create_task(channel.start())
    await asyncio.wait_for(entered.wait(), 2)
    starting.cancel()
    await asyncio.wait_for(broker.closing.wait(), 2)
    starting.cancel()
    restarting = asyncio.create_task(channel.start())
    await asyncio.sleep(0)
    assert not starting.done() and not restarting.done()
    gate.set()
    broker.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(starting, 2)
    await asyncio.wait_for(restarting, 2)
    sessions = broker.consumers if kind == "kafka" else broker.connections
    assert len(sessions) == 2
    assert sessions[0].stopped if kind == "kafka" else sessions[0].closed
    broker.feed("recovered")
    assert (await receive(channel)).event_name == "recovered"


async def test_full_pipeline_stop_restart_preserves_every_handoff(transport):
    kind, (broker, mailbox) = transport
    builtin = BuiltinChannel(1)
    pipe = ApixEventPipe(builtin=builtin, mailbox=mailbox,
                         mailtruck=Gateway(), remote_enabled=True)
    await pipe.put(event("local"))
    try:
        await pipe.start()
        for name in ("pending", "buffered", "unadmitted"):
            broker.feed(name)
        for _ in range(3):
            await asyncio.wait_for(broker.delivered.get(), 2)
        assert builtin.full() and mailbox.full()
        await asyncio.wait_for(pipe.stop(), 2)
        if kind == "kafka":
            assert broker.committed == {TopicPartition("mailbox.node", 0): 2}
        else:
            assert broker.acked == ["pending", "buffered"]
            assert broker.rejected == [("unadmitted", True)]
        assert (await receive(pipe)).event_name == "local"
        await pipe.start()
        assert [(await receive(pipe)).event_name for _ in range(3)] == [
            "pending", "buffered", "unadmitted",
        ]
        await asyncio.wait_for(mailbox.join(), 2)
        await asyncio.wait_for(pipe.join(), 2)
    finally:
        await pipe.stop()


async def test_consumer_dying_during_online_broadcast_rolls_back(transport):
    kind, (broker, mailbox) = transport
    gateway = Gateway()
    gateway.allow_online.clear()
    pipe = ApixEventPipe(mailbox=mailbox, mailtruck=gateway, remote_enabled=True)
    starting = asyncio.create_task(pipe.start())
    await asyncio.wait_for(gateway.online_entered.wait(), 2)
    incoming = broker.consumers[-1].inbox if kind == "kafka" else broker.pending
    incoming.put_nowait(RuntimeError("receiver died during startup"))
    await eventually(lambda: not mailbox.is_running)
    gateway.allow_online.set()
    with pytest.raises(EventChannelUnavailableError, match="exited during startup"):
        await asyncio.wait_for(starting, 2)
    assert gateway.actions == ["start", "fetch", "online", "offline"]
    assert gateway.closed == 1 and not pipe.is_running
    await pipe.start()
    assert pipe.is_running
    await pipe.stop()

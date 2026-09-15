"""Event channel capabilities, transports, and wire serialization."""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

import httpx
from apixis.core.config.core_config import EVENT_PIPE_MAX_LEN
from apixis.core.event.base import ApixEvent, ApixEventError, EventType
from apixis.core.utils.exception import EventChannelUnavailableError
from apixis.core.utils.logger import logger


def event_to_json(event: ApixEvent) -> dict[str, Any]:
    """Convert an :class:`ApixEvent` to its wire representation."""
    if not isinstance(event, ApixEvent):
        raise TypeError(
            "External event channels only accept ApixEvent instances, "
            f"got {type(event).__name__}."
        )
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "event_name": event.event_name,
        "context": event.context,
        "timestamp": event.timestamp,
        "accepted": event.accepted,
        "seen": event.seen,
        "error_stack": [asdict(error) for error in event.error_stack],
    }


def event_from_json(payload: Any) -> ApixEvent:
    """Deserialize a broker or gateway payload into an :class:`ApixEvent`."""
    if isinstance(payload, ApixEvent):
        return payload
    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload = bytes(payload).decode("utf-8")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise TypeError(
            "External event payload must be a mapping, JSON string, or bytes."
        )

    # A gateway may retain its routing envelope when publishing to a mailbox.
    if isinstance(payload.get("event"), Mapping):
        payload = payload["event"]

    required = {"event_id", "event_type", "event_name", "timestamp"}
    missing = required.difference(payload)
    if missing:
        raise ValueError(
            "External event payload is missing fields: "
            + ", ".join(sorted(missing))
        )

    return ApixEvent(
        event_id=str(payload["event_id"]),
        event_type=EventType(payload["event_type"]),
        event_name=str(payload["event_name"]),
        context=payload.get("context"),
        timestamp=float(payload["timestamp"]),
        accepted=bool(payload.get("accepted", False)),
        seen=list(payload.get("seen", [])),
        error_stack=[
            ApixEventError(**error)
            for error in payload.get("error_stack", [])
        ],
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def encode_event(event: ApixEvent) -> bytes:
    """Encode an event for Kafka or RabbitMQ."""
    return json.dumps(
        event_to_json(event),
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


class BaseEventChannel(ABC):
    """Lifecycle shared by all event channels, independent of I/O capabilities."""

    async def start(self) -> None:
        """Open connections and start background consumers when required."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections and background tasks."""


class ReadableEventChannel(BaseEventChannel):
    """Buffered event receiver with queue inspection and acknowledgement."""

    @property
    @abstractmethod
    def maxsize(self) -> int:
        """Maximum buffered event count. Zero means unbounded."""

    @abstractmethod
    async def get(self) -> Any:
        """Wait for and retrieve an event."""

    @abstractmethod
    def get_nowait(self) -> Any:
        """Retrieve an event without waiting."""

    @abstractmethod
    def empty(self) -> bool:
        """Return whether no buffered event is available."""

    @abstractmethod
    def full(self) -> bool:
        """Return whether the local buffer is full."""

    @abstractmethod
    def qsize(self) -> int:
        """Return the local buffered event count."""

    @abstractmethod
    def task_done(self) -> None:
        """Mark a retrieved event as processed."""

    @abstractmethod
    async def join(self) -> None:
        """Wait until all retrieved events have been processed."""


class WritableEventChannel(BaseEventChannel):
    """Asynchronous event sender without a local queue requirement."""

    @abstractmethod
    async def put(self, event: Any, **kwargs: Any) -> None:
        """Send an event, awaiting capacity or transport completion as needed."""


class ReadWriteEventChannel(ReadableEventChannel, WritableEventChannel):
    """Local read/write queue that additionally supports immediate publication."""

    @abstractmethod
    def put_nowait(self, event: Any) -> None:
        """Push an event without waiting, raising QueueFull if at capacity."""


class BuiltinChannel(ReadWriteEventChannel):
    """In-process event channel backed by :class:`asyncio.Queue`."""

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    async def put(self, event: Any, **kwargs: Any) -> None:
        await self._queue.put(event)

    def put_nowait(self, event: Any) -> None:
        self._queue.put_nowait(event)

    async def get(self) -> Any:
        return await self._queue.get()

    def get_nowait(self) -> Any:
        return self._queue.get_nowait()

    def empty(self) -> bool:
        return self._queue.empty()

    def full(self) -> bool:
        return self._queue.full()

    def qsize(self) -> int:
        return self._queue.qsize()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    async def close(self) -> None:
        return None


class _BufferedMailboxChannel(ReadableEventChannel):
    """Common local-buffer behaviour for receive-only broker channels."""

    def __init__(self, maxsize: int) -> None:
        self._buffer: asyncio.Queue[ApixEvent] = asyncio.Queue(maxsize=maxsize)

    @property
    def maxsize(self) -> int:
        return self._buffer.maxsize

    async def _enqueue(self, payload: Any) -> None:
        """Deserialize and enqueue one broker message.

        Invalid external payloads are isolated to the current message so they
        cannot terminate the broker consumer.
        """
        try:
            event = event_from_json(payload)
        except (UnicodeDecodeError, TypeError, ValueError) as exc:
            logger.warning(f"{type(self).__name__} discarded an invalid external event: {exc}")
            return

        await self._buffer.put(event)

    async def _close_consumer_task(
        self,
        task: asyncio.Task[None] | None,
    ) -> None:
        """Cancel and collect a broker consumer without leaking its failure."""
        if task is None:
            return

        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception(f"{type(self).__name__} consumer task exited unexpectedly.")

    async def get(self) -> ApixEvent:
        return await self._buffer.get()

    def get_nowait(self) -> ApixEvent:
        return self._buffer.get_nowait()

    def empty(self) -> bool:
        return self._buffer.empty()

    def full(self) -> bool:
        return self._buffer.full()

    def qsize(self) -> int:
        return self._buffer.qsize()

    def task_done(self) -> None:
        self._buffer.task_done()

    async def join(self) -> None:
        await self._buffer.join()


class KafkaChannel(_BufferedMailboxChannel):
    """Kafka mailbox consumer. ``aiokafka`` is imported only when started."""

    def __init__(
        self,
        *,
        mq_id: str,
        bootstrap_servers: str | list[str],
        topic_prefix: str,
        group_id_prefix: str,
        maxsize: int = EVENT_PIPE_MAX_LEN,
    ) -> None:
        super().__init__(maxsize)
        self.mq_id = mq_id
        self.topic = f"{topic_prefix}.{mq_id}"
        self.group_id = f"{group_id_prefix}.{mq_id}"
        self.bootstrap_servers = bootstrap_servers
        self._consumer: Any = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError as exc:  # pragma: no cover - depends on deployment
            raise EventChannelUnavailableError(
                "Kafka mailbox requires the `aiokafka` package. Run `uv add aiokafka` to install."
            ) from exc

        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        await self._consumer.start()
        self._consumer_task = asyncio.create_task(
            self._consume(), name=f"kafka-mailbox-{self.mq_id}"
        )

    async def _consume(self) -> None:
        async for record in self._consumer:
            await self._enqueue(record.value)

    async def close(self) -> None:
        consumer_task = self._consumer_task
        self._consumer_task = None

        await self._close_consumer_task(consumer_task)

        consumer = self._consumer
        self._consumer = None

        if consumer is not None:
            await consumer.stop()


class RabbitMQChannel(_BufferedMailboxChannel):
    """RabbitMQ mailbox consumer. ``aio-pika`` is imported when started."""

    def __init__(
        self,
        *,
        mq_id: str,
        url: str,
        exchange: str,
        queue_prefix: str,
        prefetch_count: int,
        maxsize: int = EVENT_PIPE_MAX_LEN,
    ) -> None:
        super().__init__(maxsize)
        self.mq_id = mq_id
        self.url = url
        self.exchange_name = exchange
        self.queue_name = f"{queue_prefix}.{mq_id}"
        self.prefetch_count = prefetch_count
        self._connection: Any = None
        self._broker_channel: Any = None
        self._broker_queue: Any = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._connection is not None:
            return
        try:
            import aio_pika
        except ImportError as exc:  # pragma: no cover - depends on deployment
            raise EventChannelUnavailableError(
                "RabbitMQ mailbox requires the `aio-pika` package. Run `uv add aio-pika` to install."
            ) from exc

        self._connection = await aio_pika.connect_robust(self.url)
        self._broker_channel = await self._connection.channel()
        await self._broker_channel.set_qos(prefetch_count=self.prefetch_count)
        exchange = await self._broker_channel.declare_exchange(
            self.exchange_name,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self._broker_queue = await self._broker_channel.declare_queue(
            self.queue_name,
            durable=True,
        )
        await self._broker_queue.bind(exchange, routing_key=self.mq_id)
        self._consumer_task = asyncio.create_task(
            self._consume(), name=f"rabbitmq-mailbox-{self.mq_id}"
        )

    async def _consume(self) -> None:
        async with self._broker_queue.iterator() as iterator:
            async for message in iterator:
                async with message.process():
                    await self._enqueue(message.body)

    async def close(self) -> None:
        consumer_task = self._consumer_task
        self._consumer_task = None

        await self._close_consumer_task(consumer_task)

        broker_channel = self._broker_channel
        connection = self._connection

        self._broker_queue = None
        self._broker_channel = None
        self._connection = None

        try:
            if broker_channel is not None:
                await broker_channel.close()
        finally:
            if connection is not None:
                await connection.close()


class UnavailableMailboxChannel(_BufferedMailboxChannel):
    """Placeholder used when remote gateway mode is disabled."""

    def __init__(self, reason: str, maxsize: int = EVENT_PIPE_MAX_LEN) -> None:
        super().__init__(maxsize)
        self.reason = reason

    async def start(self) -> None:
        return None

    async def get(self) -> ApixEvent:
        raise EventChannelUnavailableError(self.reason)

    def get_nowait(self) -> ApixEvent:
        raise EventChannelUnavailableError(self.reason)

    async def close(self) -> None:
        return None


class GatewayChannel(WritableEventChannel):
    """Write-only HTTP channel used to ask the gateway to route events."""

    def __init__(
        self,
        *,
        base_url: str,
        pipe_endpoint: str,
        node_id: str,
        node_name: str,
        channel_type: str,
        max_retry: int,
        retry_initial_delay: float,
        timeout: float,
        client: Any = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.pipe_endpoint = "/" + pipe_endpoint.lstrip("/")
        self.node_id = node_id
        self.node_name = node_name
        self.channel_type = channel_type
        self.max_retry = max_retry
        self.retry_initial_delay = retry_initial_delay
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    @property
    def url(self) -> str:
        return f"{self.base_url}{self.pipe_endpoint}"

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)

    async def _request(self, method: str, **kwargs: Any) -> httpx.Response:
        await self.start()
        assert self._client is not None

        for retry in range(self.max_retry + 1):
            try:
                response = await self._client.request(method, self.url, **kwargs)
            except httpx.RequestError:
                if retry >= self.max_retry:
                    raise
            else:
                if response.status_code != 503:
                    response.raise_for_status()
                    return response
                if retry >= self.max_retry:
                    response.raise_for_status()

            await asyncio.sleep(self.retry_initial_delay * (2**retry))

        raise AssertionError("gateway retry loop ended unexpectedly")

    def _sender(self) -> dict[str, str]:
        return {
            "tag": self.node_name,
            "node_id": self.node_id,
            "channel_type": self.channel_type,
        }

    async def put(self, event: Any, **kwargs: Any) -> None:
        recipient = kwargs.get("recipient")
        if not isinstance(recipient, str) or not recipient.strip():
            raise ValueError("mailtruck requires a non-empty recipient mq_id")
        await self._request(
            "POST",
            json={
                "action": "route",
                "sender": self._sender(),
                "recipient": recipient,
                "event": event_to_json(event),
            },
        )

    async def broadcast(self, event: ApixEvent) -> dict[str, Any]:
        response = await self._request(
            "POST",
            json={
                "action": "broadcast",
                "sender": self._sender(),
                "event": event_to_json(event),
            },
        )
        try:
            data = response.json()
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    async def fetch_nodes(self) -> dict[str, dict[str, Any]]:
        response = await self._request(
            "GET",
            params={
                "action": "nodes",
                "node_id": self.node_id,
            },
        )
        try:
            data = response.json()
        except json.JSONDecodeError:
            return {}
        if isinstance(data, Mapping):
            data = data.get("nodes", data)

        nodes: dict[str, dict[str, Any]] = {}
        if isinstance(data, Mapping):
            for key, value in data.items():
                if isinstance(value, Mapping):
                    node = dict(value)
                    mq_id = str(node.get("node_id", key))
                    nodes[mq_id] = node
        elif isinstance(data, list):
            for value in data:
                if isinstance(value, Mapping) and value.get("node_id"):
                    node = dict(value)
                    nodes[str(node["node_id"])] = node
        return nodes

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None


__all__ = [
    "BaseEventChannel",
    "ReadableEventChannel",
    "WritableEventChannel",
    "ReadWriteEventChannel",
    "BuiltinChannel",
    "GatewayChannel",
    "KafkaChannel",
    "RabbitMQChannel",
    "UnavailableMailboxChannel",
    "event_to_json",
    "event_from_json",
    "encode_event",
]
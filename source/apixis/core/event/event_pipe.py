"""Node-side event pipe with isolated transport channels.

``ApixEventPipe`` accepts local events into an unbounded ready queue while
isolating all external transport details behind mailbox and mailtruck
channels.  The event loop therefore only consumes the builtin channel and
does not need to know whether an event originated locally, from Kafka, or from
RabbitMQ.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any, Literal, TypedDict, overload
from uuid import uuid4

from apixis.core.config.core_config import (
    EVENT_CHANNEL_CONFIG,
    EVENT_CHANNEL_TYPE,
    GATEWAY_MAX_RETRY,
    GATEWAY_RETRY_INITIAL_DELAY,
    GATEWAY_TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID_PREFIX,
    KAFKA_TOPIC_PREFIX,
    NODE_ID,
    NODE_NAME,
    RABBITMQ_EXCHANGE,
    RABBITMQ_PREFETCH_COUNT,
    RABBITMQ_QUEUE_PREFIX,
    RABBITMQ_URL,
    REMOTE_GATEWAY_BASE_URL,
    REMOTE_GATEWAY_ENABLE,
    REMOTE_GATEWAY_PIPE_ENDPOINT,
)
from apixis.core.event.base import ApixEvent, ChannelType, EventType
from apixis.core.event.pipe_channel import (
    BuiltinChannel,
    GatewayChannel,
    KafkaChannel,
    RabbitMQChannel,
    ReadableEventChannel,
    ReadWriteEventChannel,
    UnavailableMailboxChannel,
    WritableEventChannel,
)
from apixis.core.utils.exception import EventChannelPermissionError
from apixis.core.utils.logger import logger


class _EventChannels(TypedDict):
    """Channel roles retain their individual capability contracts."""

    builtin: ReadWriteEventChannel
    mailbox: ReadableEventChannel
    mailtruck: WritableEventChannel


class ApixEventPipe:
    """Node-side event pipe with an unbounded builtin ready channel.

    Local publication never waits for dispatch capacity. The event loop owns
    processing backpressure, so handlers can safely publish follow-up events.
    Custom builtin channels must also provide unbounded buffering.
    """

    def __init__(
        self,
        *,
        builtin: ReadWriteEventChannel | None = None,
        mailbox: ReadableEventChannel | None = None,
        mailtruck: WritableEventChannel | None = None,
        remote_enabled: bool = REMOTE_GATEWAY_ENABLE,
        mq_id: str = NODE_ID,
        node_name: str = NODE_NAME,
        channel_type: str = EVENT_CHANNEL_TYPE,
    ) -> None:
        if builtin is not None and builtin.maxsize != 0:
            raise ValueError("The builtin ready channel must be unbounded (maxsize=0).")
        self.remote_enabled = remote_enabled
        self.mq_id = mq_id
        self.node_name = node_name
        self.channel_type = channel_type
        self._event_pipe: _EventChannels = {
            "builtin": builtin if builtin is not None else BuiltinChannel(),
            "mailbox": mailbox or self._build_mailbox(channel_type),
            "mailtruck": mailtruck or GatewayChannel(
                base_url=REMOTE_GATEWAY_BASE_URL,
                pipe_endpoint=REMOTE_GATEWAY_PIPE_ENDPOINT,
                node_id=mq_id,
                node_name=node_name,
                channel_type=channel_type,
                max_retry=GATEWAY_MAX_RETRY,
                retry_initial_delay=GATEWAY_RETRY_INITIAL_DELAY,
                timeout=GATEWAY_TIMEOUT,
            ),
        }
        self._mailbox_forwarder: asyncio.Task[None] | None = None
        self._nodes: dict[str, dict[str, Any]] = {}
        self._started = False

    def _build_mailbox(self, channel_type: str) -> ReadableEventChannel:
        if not self.remote_enabled:
            return UnavailableMailboxChannel(
                "mailbox is unavailable while REMOTE_GATEWAY is disabled"
            )
        if channel_type == "kafka":
            return KafkaChannel(
                mq_id=self.mq_id,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                topic_prefix=KAFKA_TOPIC_PREFIX,
                group_id_prefix=KAFKA_GROUP_ID_PREFIX,
            )
        if channel_type == "rabbitmq":
            return RabbitMQChannel(
                mq_id=self.mq_id,
                url=RABBITMQ_URL,
                exchange=RABBITMQ_EXCHANGE,
                queue_prefix=RABBITMQ_QUEUE_PREFIX,
                prefetch_count=RABBITMQ_PREFETCH_COUNT,
            )
        raise ValueError(
            "EVENT_CHANNEL.type must be either 'kafka' or 'rabbitmq', "
            f"got {channel_type!r}."
        )

    @property
    def maxsize(self) -> int:
        return self._event_pipe["builtin"].maxsize

    @property
    def nodes(self) -> dict[str, dict[str, Any]]:
        return {node_id: dict(node) for node_id, node in self._nodes.items()}

    @overload
    def get_channel(self, channel: Literal["builtin"]) -> ReadWriteEventChannel: ...

    @overload
    def get_channel(self, channel: Literal["mailbox"]) -> ReadableEventChannel: ...

    @overload
    def get_channel(self, channel: Literal["mailtruck"]) -> WritableEventChannel: ...

    @overload
    def get_channel(
        self, channel: Literal["builtin", "mailbox"],
    ) -> ReadableEventChannel: ...

    @overload
    def get_channel(
        self, channel: ChannelType,
    ) -> ReadableEventChannel | WritableEventChannel: ...

    def get_channel(
        self, channel: ChannelType,
    ) -> ReadableEventChannel | WritableEventChannel:
        try:
            return self._event_pipe[channel]
        except KeyError as exc:
            raise ValueError(f"Unknown event channel: {channel!r}") from exc

    def _get_read_channel(self, channel: ChannelType) -> ReadableEventChannel:
        """Validate the pipe role before accessing a buffered receiver."""
        if channel == "mailtruck":
            raise EventChannelPermissionError("mailtruck channels are write-only")
        return self.get_channel(channel)

    async def put(
        self,
        event: Any,
        channel: ChannelType = "builtin",
        *,
        recipient: str | None = None,
    ) -> None:
        if channel == "mailbox":
            raise EventChannelPermissionError("mailbox channels are receive-only")
        if channel == "mailtruck":
            await self.get_channel(channel).put(event, recipient=recipient)
        else:
            target_channel = self.get_channel(channel)
            await target_channel.put(event)

    async def post_event(
        self,
        *,
        event_type: EventType,
        event_name: str,
        context: Any = None,
        channel: ChannelType = "builtin",
        recipient: str | None = None,
    ) -> None:
        """Create and post an event to the selected channel.

        Local events enter the unbounded ready queue; this does not wait for
        processing capacity or handler completion.

        Args:
            event_type: Event category used by handlers and transports.
            event_name: Name used by the registry to select handlers.
            context: Optional event payload or runtime context.
            channel: Target event channel. Local events use ``builtin``.
            recipient: Destination node MQ id when using ``mailtruck``.
        """
        event = ApixEvent(
            event_id="event-" + uuid4().hex,
            event_type=event_type,
            event_name=event_name,
            context=context,
            timestamp=time.time(),
            accepted=False,
        )
        await self.put(event, channel, recipient=recipient)

    def put_nowait(
        self,
        event: Any,
        channel: ChannelType = "builtin",
    ) -> None:
        if channel == "mailbox":
            raise EventChannelPermissionError("mailbox channels are receive-only")
        if channel == "mailtruck":
            raise EventChannelPermissionError(
                "mailtruck performs asynchronous HTTP writes; use await put()"
            )
        target_channel = self.get_channel(channel)
        target_channel.put_nowait(event)

    async def get(self, channel: ChannelType = "builtin") -> Any:
        return await self._get_read_channel(channel).get()

    def get_nowait(self, channel: ChannelType = "builtin") -> Any:
        return self._get_read_channel(channel).get_nowait()

    def empty(self, channel: ChannelType = "builtin") -> bool:
        return self._get_read_channel(channel).empty()

    def full(self, channel: ChannelType = "builtin") -> bool:
        return self._get_read_channel(channel).full()

    def qsize(self, channel: ChannelType = "builtin") -> int:
        return self._get_read_channel(channel).qsize()

    def task_done(self, channel: ChannelType = "builtin") -> None:
        self._get_read_channel(channel).task_done()

    async def join(self, channel: ChannelType = "builtin") -> None:
        await self._get_read_channel(channel).join()

    async def clear(self, channel: ChannelType = "builtin") -> int:
        """Remove and acknowledge all currently queued events."""
        count = 0
        while not self.empty(channel):
            await self.get(channel)
            self.task_done(channel)
            count += 1
        logger.info(f"Cleaned {count} in event pipe.")
        return count

    async def send(self, event: ApixEvent, recipient: str) -> None:
        """Route an event to another node through the gateway."""
        await self.put(event, "mailtruck", recipient=recipient)

    async def broadcast(self, event: ApixEvent) -> dict[str, Any]:
        """Broadcast a node lifecycle event through the gateway."""
        if not self.remote_enabled:
            return {}
        mailtruck = self.get_channel("mailtruck")
        if not isinstance(mailtruck, GatewayChannel) and not hasattr(
            mailtruck, "broadcast"
        ):
            raise TypeError("mailtruck channel does not support broadcast()")
        result = await mailtruck.broadcast(event)  # type: ignore[attr-defined]
        self._update_nodes(result)
        return result

    def _update_nodes(self, payload: Any) -> None:
        if isinstance(payload, Mapping):
            payload = payload.get("nodes", payload)
        if isinstance(payload, Mapping):
            for key, value in payload.items():
                if isinstance(value, Mapping):
                    node = dict(value)
                    node_id = str(node.get("node_id", key))
                    self._nodes[node_id] = node
        elif isinstance(payload, list):
            for value in payload:
                if isinstance(value, Mapping) and value.get("node_id"):
                    node = dict(value)
                    self._nodes[str(node["node_id"])] = node

    def _lifecycle_event(self, online: bool) -> ApixEvent:
        status = "ok" if online else "unavailable"
        return ApixEvent(
            event_id="event-" + uuid4().hex,
            event_type=EventType.LIFECYCLE,
            event_name="apixis.node.online" if online else "apixis.node.offline",
            context={
                "tag": self.node_name,
                "node_id": self.mq_id,
                "channel_config": EVENT_CHANNEL_CONFIG,
                "status": status,
            },
            timestamp=time.time(),
            accepted=False,
        )

    async def _forward_mailbox(self) -> None:
        mailbox = self.get_channel("mailbox")
        builtin = self.get_channel("builtin")
        while True:
            event = await mailbox.get()
            try:
                await builtin.put(event)
            finally:
                mailbox.task_done()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        try:
            await self.get_channel("builtin").start()
            if self.remote_enabled:
                await self.get_channel("mailtruck").start()
                await self.get_channel("mailbox").start()
                self._mailbox_forwarder = asyncio.create_task(
                    self._forward_mailbox(),
                    name=f"mailbox-forwarder-{self.mq_id}",
                )
                await self.broadcast(self._lifecycle_event(online=True))
                mailtruck = self.get_channel("mailtruck")
                if hasattr(mailtruck, "fetch_nodes"):
                    self._update_nodes(
                        await mailtruck.fetch_nodes()  # type: ignore[attr-defined]
                    )
        except BaseException:
            await self._close_channels()
            self._started = False
            raise

    async def _close_channels(self) -> list[BaseException]:
        errors: list[BaseException] = []
        if self._mailbox_forwarder is not None:
            self._mailbox_forwarder.cancel()
            forwarder_result = await asyncio.gather(
                self._mailbox_forwarder,
                return_exceptions=True,
            )
            errors.extend(
                result
                for result in forwarder_result
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            )
            self._mailbox_forwarder = None

        results = await asyncio.gather(
            *(
                self.get_channel(channel_name).close()
                for channel_name in ("mailbox", "mailtruck", "builtin")
            ),
            return_exceptions=True,
        )
        errors.extend(
            result for result in results if isinstance(result, BaseException)
        )
        return errors

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        errors: list[BaseException] = []
        if self.remote_enabled:
            try:
                await self.broadcast(self._lifecycle_event(online=False))
            except Exception as exc:
                errors.append(exc)

        errors.extend(await self._close_channels())
        if errors:
            raise errors[0]


__all__ = ["ApixEventPipe"]

"""Node-side event pipe with isolated transport channels.

``ApixEventPipe`` accepts local events into a bounded queue while
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
    EVENT_PIPE_MAX_LEN,
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
from apixis.core.event.base import (
    ApixEvent, ChannelType, EventType, suspend_process,
)
from apixis.core.event.pipe_channel import (
    BuiltinChannel,
    GatewayChannel,
    KafkaChannel,
    RabbitMQChannel,
    ReadableEventChannel,
    ReadWriteEventChannel,
    UnavailableMailboxChannel,
    WritableEventChannel,
    _complete_cleanup,
)
from apixis.core.utils.exception import (
    EventChannelPermissionError, EventChannelUnavailableError,
)
from apixis.core.utils.logger import logger


class _EventChannels(TypedDict):
    """Channel roles retain their individual capability contracts."""

    builtin: ReadWriteEventChannel
    mailbox: ReadableEventChannel
    mailtruck: WritableEventChannel


class ApixEventPipe:
    """Node-side event pipe with a bounded builtin channel.

    Publication waits when the queue is full. post_event() temporarily returns
    the current handler's permit so follow-up events can continue to drain.
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
        if builtin is None:
            if EVENT_PIPE_MAX_LEN <= 0:
                raise ValueError("EVENT_PIPE_MAX_LEN must be positive.")
            builtin = BuiltinChannel(maxsize=EVENT_PIPE_MAX_LEN)
        elif builtin.maxsize <= 0:
            raise ValueError("The builtin channel must be bounded (maxsize > 0).")
        self.remote_enabled = remote_enabled
        self.mq_id = mq_id
        self.node_name = node_name
        self.channel_type = channel_type
        self._event_pipe: _EventChannels = {
            "builtin": builtin,
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
        # A dequeued mailbox event remains owned here until builtin accepts it.
        # Stopping the forwarder must preserve both the event and its pending ack.
        self._pending_mailbox_event: ApixEvent | None = None
        self._lifecycle_lock = asyncio.Lock()
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
    def is_running(self) -> bool:
        """Return whether this pipe's receiving tasks are still available."""
        if not self._started:
            return False
        if not self.remote_enabled:
            return True
        return (
            self.get_channel("mailbox").is_running
            and self._mailbox_forwarder is not None
            and not self._mailbox_forwarder.done()
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

        Local publication waits for queue capacity, not event completion.
        Within a managed handler, return its permit during publication and
        reacquire it before continuing, including after errors or cancellation.

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
        async with suspend_process():
            await self.put(event, channel, recipient=recipient)

    def put_nowait(
        self,
        event: ApixEvent,
        channel: ChannelType = "builtin",
    ) -> None:
        """Push an event without waiting, raising QueueFull if at capacity."""
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
            if self._pending_mailbox_event is None:
                self._pending_mailbox_event = await mailbox.get()
            await builtin.put(self._pending_mailbox_event)
            # No await separates the successful handoff from acknowledgement.
            self._pending_mailbox_event = None
            mailbox.task_done()

    def _on_forwarder_done(self, task: asyncio.Task[None]) -> None:
        """Report forwarding failures immediately; health checks detect task exit."""
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(f"Mailbox forwarding failed: {type(error).__name__}: {error}")

    async def start(self) -> None:
        """Start once, rebuilding receivers that have exited since startup."""
        async with self._lifecycle_lock:
            if self.is_running:
                return
            if self._started:
                if self._mailbox_forwarder is not None and self._mailbox_forwarder.done():
                    # Its failure has already been reported. Retiring a dead worker
                    # must not make the first recovery attempt fail again.
                    if not self._mailbox_forwarder.cancelled():
                        self._mailbox_forwarder.exception()
                    self._mailbox_forwarder = None
                await self._stop()
            online_attempted = False
            try:
                await self.get_channel("builtin").start()
                if self.remote_enabled:
                    mailtruck = self.get_channel("mailtruck")
                    await mailtruck.start()
                    await self.get_channel("mailbox").start()
                    self._mailbox_forwarder = asyncio.create_task(
                        self._forward_mailbox(),
                        name=f"mailbox-forwarder-{self.mq_id}",
                    )
                    self._mailbox_forwarder.add_done_callback(self._on_forwarder_done)
                    if hasattr(mailtruck, "fetch_nodes"):
                        self._update_nodes(
                            await mailtruck.fetch_nodes()  # type: ignore[attr-defined]
                        )
                    # Publish availability last. Even a failed/cancelled HTTP request
                    # may already have reached the gateway and needs compensation.
                    online_attempted = True
                    await self.broadcast(self._lifecycle_event(online=True))
                self._started = True
                if not self.is_running:
                    raise EventChannelUnavailableError("Mailbox receiver exited during startup.")
            except BaseException:
                self._started = False
                errors = await self._close_channels(announce_offline=online_attempted)
                for error in errors:
                    logger.error(f"Startup rollback failed: {type(error).__name__}: {error}")
                raise

    async def _close_channels(self, *, announce_offline: bool = False) -> list[BaseException]:
        """Finish rollback and cleanup before propagating repeated cancellation."""
        return await _complete_cleanup(
            self._close_resources(announce_offline=announce_offline),
            name=f"pipe-cleanup-{self.mq_id}",
        )

    async def _close_resources(self, *, announce_offline: bool = False) -> list[BaseException]:
        """Stop forwarding and close every channel, collecting individual failures."""
        errors: list[BaseException] = []
        if announce_offline:
            try:
                await self.broadcast(self._lifecycle_event(online=False))
            except BaseException as exc:
                # A failed compensation must not prevent transport cleanup.
                errors.append(exc)
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
        """Close transports while preserving queued and pending mailbox events.

        Cancellation is propagated after cleanup finishes. Concurrent start()
        and stop() calls wait for that cleanup rather than reopening resources.
        """
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self) -> None:
        """Retire an owned session, including one with a failed receiver."""
        if not self._started:
            return
        self._started = False
        errors: list[BaseException] = []
        try:
            if self.remote_enabled:
                try:
                    await self.broadcast(self._lifecycle_event(online=False))
                except Exception as exc:
                    errors.append(exc)
        finally:
            errors.extend(await self._close_channels())
        if errors:
            raise errors[0]


__all__ = ["ApixEventPipe"]

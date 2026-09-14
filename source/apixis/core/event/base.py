import asyncio
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Literal, Self
from uuid import uuid4

from apixis.core.utils.logger import logger


class EventType(str, Enum):
    INTERNAL = 'internal' # Internal event type for event bus itself.
    WORKFLOW = 'workflow'
    LIFECYCLE = 'lifecycle'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'


@dataclass(frozen=True, slots=True)
class ApixEventError:
    """Serializable failure details without references to live traceback frames."""

    handler_name: str
    phase: Literal["core_func", "on_has_error", "on_accepted", "on_error"]
    exception_type: str
    message: str
    traceback: str


@dataclass(slots=True)
class ApixEvent:
    event_id: str
    event_type: EventType
    event_name: str
    context: Any
    timestamp: float
    accepted: bool = False
    seen: list[str] = field(default_factory=list) # List of handler names that have processed this event.
    error_stack: list[ApixEventError] = field(default_factory=list)

    def accept(self) -> None:
        '''
        Mark this event item as accepted.

        Once accepted, subsequent handlers skip their core functions but
        still receive applicable error and acceptance notifications.
        '''
        self.accepted = True

    @property
    def has_error(self) -> bool:
        """Return whether a foreground handler has recorded a failure."""
        return bool(self.error_stack)

    @property
    def datetime(self) -> datetime:
        '''
        Convert timestamp to datetime object.
        '''
        return datetime.fromtimestamp(self.timestamp)


EventHandlerFunc = Callable[[ApixEvent], Awaitable[None]]
EventHandlerErrorFunc = Callable[[ApixEvent, Exception], Awaitable[None]]


class ApixEventHandler:
    """Execute a core function with notifications about preceding handlers.

    ``on_has_error`` handles upstream failures only. ``on_error`` receives
    this handler's own uncaught exception after it has been logged and, for
    foreground handlers, recorded. Local try/except/finally blocks may still
    handle recovery and cleanup without reporting an event failure.
    ``on_cancelled`` receives event cancellation notifications from the event
    loop, independently of the normal handler chain. Background cancellation
    only notifies the cancelled background handler.
    Registration metadata is assigned by :func:`subscribe` or :meth:`register`.
    """

    id: str
    name: str
    subscribe: list[str]
    filter_event: list[str]
    core_func: EventHandlerFunc
    on_accepted: EventHandlerFunc
    on_has_error: EventHandlerFunc
    on_error: EventHandlerErrorFunc
    on_cancelled: EventHandlerFunc
    stop_when_error: bool
    time_out: float | None
    background: bool
    priority: float | None
    between_handlers: tuple[str | None, str | None] | None
    _register_order: float

    def __init__(
        self,
        core_func: EventHandlerFunc,
        on_accepted: EventHandlerFunc | None = None,
        on_has_error: EventHandlerFunc | None = None,
        on_error: EventHandlerErrorFunc | None = None,
        on_cancelled: EventHandlerFunc | None = None,
        *,
        stop_when_error: bool = True,
        time_out: float | None = None,
        background: bool = False,
        name: str | None = None,
    ) -> None:
        for callback in (core_func, on_accepted, on_has_error, on_error, on_cancelled):
            if callback is not None and not callable(callback):
                raise TypeError("Handler functions must be callable.")
        if core_func is None:
            raise TypeError("core_func must be callable.")
        self.core_func: EventHandlerFunc = core_func
        self.on_accepted: EventHandlerFunc | None = on_accepted
        self.on_has_error: EventHandlerFunc | None = on_has_error
        self.on_error: EventHandlerErrorFunc | None = on_error
        self.on_cancelled: EventHandlerFunc | None = on_cancelled
        self.stop_when_error = stop_when_error
        self.time_out = time_out if time_out is not None and time_out > 0 else None
        self.background = background
        self.name = name or getattr(core_func, "__name__", type(core_func).__name__)
        self.id = "handler-" + uuid4().hex
        self._register_order = -1
        self.subscribe: list[str] = []
        self.filter_event: list[str] = []
        self.priority: float | None = None
        self.between_handlers: tuple[str | None, str | None] | None = None

    @property
    def __name__(self) -> str:
        """Expose the registered name for decorator and inspection tools."""
        return self.name

    async def __call__(self, event: ApixEvent) -> None:
        """Use the same execution contract when called directly."""
        await self.execute(event)

    async def execute(self, event: ApixEvent) -> None:
        """Notify about upstream state, then conditionally execute the core.

        Error notification precedes acceptance notification when both apply.
        Each upstream notification runs at most once. State is checked again after
        notification, so accepting the event there also suppresses the core.
        A timeout applies separately to each invoked function. Cancellation
        propagates; other failures are logged and foreground failures are
        appended to the event without terminating dispatch.

        Each failed core or upstream notification calls ``on_error(event, exc)``
        once. A failure in ``on_error`` is recorded without recursive handling.
        Successful error handling does not remove the original error or retry
        the failed function. Cancellation propagates without calling on_error.
        Background failures still invoke on_error but never enter error_stack.
        """
        if event.has_error and self.on_has_error is not None:
            await self._execute_func(self.on_has_error, "on_has_error", event)
        if event.accepted:
            if self.on_accepted is not None:
                await self._execute_func(self.on_accepted, "on_accepted", event)
            return
        if event.has_error and self.stop_when_error:
            return
        event.seen.append(self.name)
        await self._execute_func(self.core_func, "core_func", event)

    async def _execute_func(
        self,
        func: Callable[..., Awaitable[None]],
        phase: Literal["core_func", "on_has_error", "on_accepted", "on_error"],
        event: ApixEvent,
        *args: Any,
    ) -> None:
        """Run one phase and report its own failure without recursive hooks."""
        try:
            if self.time_out is None:
                await func(event, *args)
            else:
                async with asyncio.timeout(self.time_out):
                    await func(event, *args)
        except asyncio.CancelledError as exc:
            error = ApixEventError(
                handler_name=self.name,
                phase=phase,
                exception_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
            if not self.background:
                event.error_stack.append(error)
            raise
        except Exception as exc:
            error = ApixEventError(
                handler_name=self.name,
                phase=phase,
                exception_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
            if not self.background:
                event.error_stack.append(error)
            logger.error(
                f"Handler failed: event={event.event_name}, "
                f"handler={self.name}, phase={phase}, "
                f"error={error.exception_type}: {error.message}\n"
                f"{error.traceback}"
            )
            if phase != "on_error" and self.on_error is not None:
                await self._execute_func(self.on_error, "on_error", event, exc)

    def set_core_func(self, callback: EventHandlerFunc) -> None:
        """Reset the core function, optionally rejecting replacement."""
        if not callable(callback):
            raise TypeError("Core function must be callable.")
        self.core_func = callback

    def add_has_error_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when an upstream handler has failed.

        The callback is invoked once per event if the event has an error in error_stack, before the core function.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self.on_has_error is not None and not exist_ok:
            raise ValueError("on_has_error already set.")
        self.on_has_error = callback

    def add_on_accepted_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when the event has been accepted.

        The callback is invoked once per event if the event has been accepted, before the core function.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self.on_accepted is not None and not exist_ok:
            raise ValueError("on_accepted already set.")
        self.on_accepted = callback

    def add_on_error_callback(self, callback: EventHandlerErrorFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when this handler has failed.

        The callback is invoked once per event if the core function fails.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self.on_error is not None and not exist_ok:
            raise ValueError("on_error already set.")
        self.on_error = callback

    async def notify_cancelled(self, event: ApixEvent) -> None:
        """Run cancellation cleanup without interrupting other notifications.

        The event loop calls this outside normal execution, then re-raises the
        original cancellation. Each hook uses this handler's timeout. Cleanup
        failures, including cancellation, are logged only: they do not invoke
        on_error, append business errors, or replace the original cancellation.
        """
        if self.on_cancelled is None:
            return
        try:
            async with asyncio.timeout(self.time_out):
                await self.on_cancelled(event)
        except (Exception, asyncio.CancelledError) as exc:
            logger.error(
                f"Cancellation cleanup failed: event={event.event_name}, "
                f"handler={self.name}, phase=on_cancelled, "
                f"error={type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            )

    def add_on_cancelled_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Set the event cancellation cleanup callback, optionally rejecting replacement."""
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self.on_cancelled is not None and not exist_ok:
            raise ValueError("on_cancelled already set.")
        self.on_cancelled = callback

    def register(
        self,
        *event_names: str,
        exist_ok: bool = True,
        priority: float | None = None,
        between_handlers: tuple[str | None, str | None] | None = None,
        filter_event: list[str] | None = None,
        stop_when_error: bool | None = None,
        time_out: float | None = None,
        background: bool | None = None,
    ) -> Self:
        """Register this instance globally with the same options as subscribe().

        Equivalent to ``subscribe(*event_names, **options)(self)``. At least
        one non-empty event-name pattern is required. Subscriptions and filters
        use case-sensitive fnmatchcase matching. Subscription, filtering and
        ordering options replace previous registration metadata; priority
        defaults to 1 unless between_handlers is supplied.

        Execution options set to None preserve this instance's settings.
        Explicit values override them; non-positive time_out disables the
        timeout. Core and notification callbacks are preserved.

        Handler names are unique across the global registry. With exist_ok=True,
        this instance replaces the same-name registration. With exist_ok=False,
        a duplicate raises EventHandlerAlreadyRegisteredError. Validation
        failures leave the instance and any existing registration unchanged.

        Returns:
            This instance after successful registration or replacement.
        """
        # Import at call time because handler_registry imports this class.
        from apixis.core.event.handler_registry import subscribe

        return subscribe(
            *event_names,
            exist_ok=exist_ok,
            priority=priority,
            between_handlers=between_handlers,
            filter_event=filter_event,
            stop_when_error=stop_when_error,
            time_out=time_out,
            background=background,
        )(self)

    def unregister(self, *, missing_ok: bool = True) -> None:
        """Immediately remove the global registration under this handler's name.

        Equivalent to ``unsubscribe(self.name, missing_ok=missing_ok)``. Remove
        the whole registration, including all subscriptions. Missing names are
        ignored by default; missing_ok=False raises EventHandlerNotRegisteredError.
        Removal is name-based, including when another instance has replaced
        this handler under the same name. Already running calls continue.
        """
        # Import at call time because handler_registry imports this class.
        from apixis.core.event.handler_registry import unsubscribe

        unsubscribe(self.name, missing_ok=missing_ok)


ChannelType = Literal["builtin", "mailbox", "mailtruck"]

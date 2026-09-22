import asyncio
from datetime import datetime
from functools import partial
import traceback

from apixis.core.config.core_config import (
    EVENT_LOOP_BACKPRESSURE,
    SHOW_EVENT_DISPATCH,
)
from apixis.core.event.base import ApixEvent, HandlerChainToken, handler_chain_context
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.utils.logger import logger


# =========================
# Common Event Handler
# =========================
class ApixEventLoop:
    """Consume events and limit concurrent foreground event dispatches."""

    def __init__(
        self,
        registry: ApixHandlerRegistry,
        event_pipe: ApixEventPipe,
        event_registry: ApixEventRegistry,
    ):
        self._registry = registry
        self._event_pipe = event_pipe
        self._event_registry = event_registry

        self._event_semaphore = asyncio.BoundedSemaphore(EVENT_LOOP_BACKPRESSURE)

        self._event_consumer_task: asyncio.Task | None = None
        self._dispatch_tasks: set[asyncio.Task] = set()
        self._background_handler_tasks: set[asyncio.Task] = set()

        # An event may already be dequeued while waiting for foreground
        # dispatch capacity. Keep ownership here so stopping the consumer
        # cannot lose the event before it is handed to a dispatch task.
        self._pending_event: ApixEvent | None = None

        self._started = False

    async def start(self) -> None:
        """Start the single local consumer in the running loop, once."""
        if self._started:
            return

        self._event_consumer_task = asyncio.create_task(
            self._event_consumer_loop(),
            name="pipe-event-consumer",
        )
        self._event_consumer_task.add_done_callback(self._on_consumer_done)
        self._started = True
        logger.info("Worker started.")

    def _on_consumer_done(self, task: asyncio.Task) -> None:
        """Reset startup state when the current consumer exits for any reason."""
        # An old consumer may finish after stop() has started a replacement.
        # Completion callbacks also cover cancellation before the coroutine runs.
        if self._event_consumer_task is task:
            self._event_consumer_task = None
            self._started = False
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(f"Event consumer failed: {type(error).__name__}: {error}")

    async def stop(self) -> None:
        """Pause consumption, preserving queued and pending events.

        Queued events remain available for restart. An event already dequeued
        but still waiting for dispatch capacity remains pending for the next
        consumer. Existing dispatch and background tasks continue without
        being awaited here.
        """
        task = self._event_consumer_task
        self._event_consumer_task = None
        self._started = False

        if task is None:
            return

        # A concurrent start() may create a replacement; only stop this worker.
        task.cancel()

        results = await asyncio.gather(task, return_exceptions=True)
        for result in results:
            if (
                isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            ):
                raise result

        logger.info("Worker stopped.")

    async def _event_consumer_loop(self) -> None:
        """Dequeue events and launch dispatch when foreground capacity is available."""
        try:
            while True:
                # Dequeue before acquiring dispatch capacity. This prevents an
                # idle consumer from occupying a slot needed by a suspended
                # foreground handler when it tries to reacquire that slot.
                if self._pending_event is None:
                    self._pending_event = await self._event_pipe.get()

                # If cancellation happens here, the dequeued event remains in
                # _pending_event and will be resumed by the next consumer.
                await self._event_semaphore.acquire()

                event = self._pending_event
                self._pending_event = None

                if event is None:
                    self._event_semaphore.release()
                    continue

                chain = HandlerChainToken(self._event_semaphore)
                try:
                    if event.event_name:
                        self._event_registry.record_event(event)

                    # Resolve synchronously after dequeue, before another task
                    # can change registrations or ordering.
                    handler_chain = (
                        self._registry.get_handlers_chain_for_event(
                            event.event_name
                        )
                        if event.event_name
                        else []
                    )

                    if SHOW_EVENT_DISPATCH:
                        print(
                            f"\033[38;5;59m"
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            f"\033[0m"
                            f" \033[1;38;5;147m[EVENT LOOP]\033[0m"
                            f" \033[38;5;59m│\033[0m"
                            f" \033[38;5;116m{event.event_name}\033[0m"
                        )

                    coroutine = self._dispatch_event(event, handler_chain)
                    token = handler_chain_context.set(chain)
                    try:
                        task = asyncio.create_task(coroutine)
                    except BaseException:
                        coroutine.close()
                        raise
                    finally:
                        handler_chain_context.reset(token)

                except BaseException as exc:
                    chain.closed = True
                    if chain.held:
                        self._event_semaphore.release()
                        chain.held = False
                    self._event_pipe.task_done()

                    if not isinstance(exc, Exception):
                        raise

                    logger.error(
                        f"Dispatch preparation failed: "
                        f"{type(exc).__name__}: {exc}\n"
                        f"{traceback.format_exc()}"
                    )

                else:
                    self._dispatch_tasks.add(task)
                    task.add_done_callback(partial(self._on_dispatch_done, chain=chain))
                    self._event_pipe.task_done()

        except asyncio.CancelledError:
            logger.info("Event loop cancelled.")

    async def _dispatch_event(
        self,
        event: ApixEvent,
        handler_chain: list[str],
    ) -> ApixEvent | None:
        """Dispatch an event and notify foreground handlers on cancellation."""
        logger.debug(
            f"Dispatching event `{event.event_name}` "
            f"to {len(handler_chain)} handlers."
        )

        try:
            if not event.event_name:
                return None

            if not handler_chain:
                return event

            for handler_name in handler_chain:
                # Earlier handlers may await while subscriptions change.
                handler = self._registry.get_handler(handler_name)

                if (
                    handler is None
                    or not self._registry._matches_handler(
                        handler,
                        event.event_name,
                    )
                ):
                    continue

                if handler.background:
                    self._create_background_handler_task(
                        handler_name,
                        event,
                    )
                else:
                    await self._execute_handler(
                        handler_name,
                        event,
                    )

            return event

        except asyncio.CancelledError:
            # Keep cancellation cleanup ordered within the same handler chain.
            for handler_name in handler_chain:
                handler = self._registry.get_handler(handler_name)

                if (
                    handler is not None
                    and not handler.background
                    and self._registry._matches_handler(
                        handler,
                        event.event_name,
                    )
                ):
                    try:
                        await handler.notify_cancelled(
                            event,
                            time_out=5.0,
                        )
                    except Exception:
                        pass

            raise

        except Exception as exc:
            logger.error(
                f"Dispatch failed: {type(exc).__name__}: "
                f"{exc}\n{traceback.format_exc()}"
            )

    def _on_dispatch_done(self, task: asyncio.Task, *, chain: HandlerChainToken) -> None:
        """Close the chain and release its permit only if it still owns one."""
        chain.closed = True
        if chain.held:
            self._event_semaphore.release()
            chain.held = False
        self._dispatch_tasks.discard(task)

        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    f"Dispatch task failed: "
                    f"{type(error).__name__}: {error}"
                )

    async def _execute_handler(
        self,
        handler_name: str,
        event: ApixEvent,
    ) -> None:
        """Execute one foreground handler in the current event dispatch."""
        try:
            # Earlier awaits may have changed the current registration.
            handler = self._registry.get_handler(handler_name)

            if (
                handler is None
                or not self._registry._matches_handler(
                    handler,
                    event.event_name,
                )
            ):
                return

            await handler.execute(event)

        except Exception as exc:
            logger.error(
                f"Foreground handler execution failed: "
                f"{type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            )

    def _create_background_handler_task(
        self,
        handler_name: str,
        event: ApixEvent,
    ) -> None:
        """Create one background handler task without foreground semaphore context."""
        coroutine = self._run_background_handler(
            handler_name,
            event,
        )

        # Background handlers do not own foreground event capacity. Clearing
        # the context also prevents their event publishing from releasing or
        # reacquiring a foreground event slot.
        token = handler_chain_context.set(None)

        try:
            task = asyncio.create_task(coroutine)
        except BaseException:
            coroutine.close()
            raise
        finally:
            handler_chain_context.reset(token)

        self._background_handler_tasks.add(task)
        task.add_done_callback(self._on_background_handler_done)

    def _on_background_handler_done(
        self,
        task: asyncio.Task,
    ) -> None:
        """Remove a completed background handler task."""
        self._background_handler_tasks.discard(task)

        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    f"Background task failed: "
                    f"{type(error).__name__}: {error}"
                )

    async def _run_background_handler(
        self,
        handler_name: str,
        event: ApixEvent,
    ) -> None:
        """Execute one background handler and notify only its own cancellation."""
        try:
            # Resolve the current entry when the background task actually runs,
            # because registration may have changed since dispatch.
            handler = self._registry.get_handler(handler_name)

            if (
                handler is None
                or not self._registry._matches_handler(
                    handler,
                    event.event_name,
                )
            ):
                return

            try:
                await handler.execute(event)

            except asyncio.CancelledError:
                # Notification failure must never replace the original
                # cancellation of the background task.
                try:
                    await handler.notify_cancelled(
                        event,
                        time_out=5.0,
                    )
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.error(
                        f"Background cancellation notification failed: "
                        f"{type(exc).__name__}: {exc}\n"
                        f"{traceback.format_exc()}"
                    )

                raise

        except Exception as exc:
            logger.error(
                f"Background handler execution failed: "
                f"{type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            )


__all__ = ["ApixEventLoop"]

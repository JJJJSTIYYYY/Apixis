import asyncio
from datetime import datetime
import traceback

from apixis.core.config.core_config import (
    BACKGROUND_HANDLER_BACKPRESSURE,
    EVENT_LOOP_BACKPRESSURE,
    SHOW_EVENT_DISPATCH
)
from apixis.core.event.base import ApixEvent
from apixis.core.event.handler_registry import (
    ApixHandlerRegistry,
)
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.utils.logger import logger


# =========================
# Common Event Handler
# =========================
class ApixEventLoop:
    """Buffer ready events and limit running dispatch tasks independently."""

    def __init__(
        self,
        registry: ApixHandlerRegistry,
        event_pipe: ApixEventPipe,
        event_registry: ApixEventRegistry,
    ):
        self._registry = registry
        self._event_pipe = event_pipe
        self._event_registry = event_registry

        self._event_consumer_task: asyncio.Task | None = None
        self._event_dispatcher_task: asyncio.Task | None = None
        min_backpressure = EVENT_LOOP_BACKPRESSURE if EVENT_LOOP_BACKPRESSURE > 128 else 128
        self._processing_queue: asyncio.Queue[ApixEvent] = (
            asyncio.Queue(maxsize=min_backpressure)
        )
        # Retain a dequeued event while put() waits, including across restarts.
        self._pending_event: ApixEvent | None = None

        self._dispatch_tasks: set[asyncio.Task] = set()
        # One permit covers a running dispatch, independently of queue capacity.
        self._dispatch_semaphore = asyncio.Semaphore(min_backpressure)

        self._background_handler_tasks: set[asyncio.Task] = set()
        self._background_handler_semaphore = asyncio.Semaphore(BACKGROUND_HANDLER_BACKPRESSURE)

        self._started = False

    async def start(self) -> None:
        """Start ready admission and event dispatch in the running loop, once."""
        if self._started:
            return
        self._started = True
        
        try:
            if self._event_dispatcher_task is None or self._event_dispatcher_task.done():
                self._event_dispatcher_task = asyncio.create_task(
                    self._event_dispatcher_loop(), name="pipe-event-dispatcher",
                )
            if self._event_consumer_task is None or self._event_consumer_task.done():
                self._event_consumer_task = asyncio.create_task(
                    self._event_consumer_loop(), name="pipe-event-consumer",
                )
            self._started = True
            logger.info("Worker started.")
        except Exception:
            self._started = False
            raise

    async def stop(self) -> None:
        """Stop queue transfer and dispatch without discarding pending events.

        Ready events, the pending transfer, and processing queue entries remain
        available for restart. Running dispatch and background tasks finish
        normally without being awaited here. Call start() to resume consumption.
        """
        if not self._started:
            return
        self._started = False

        try:
            task = self._event_consumer_task
            dispatcher = self._event_dispatcher_task
            # A concurrent start() may create new workers. Only stop the
            # captured workers, and leave any replacement references intact.
            self._event_consumer_task = None
            self._event_dispatcher_task = None
            self._started = False
            workers = [worker for worker in (task, dispatcher) if worker is not None]
            for worker in workers:
                worker.cancel()
            results = await asyncio.gather(*workers, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                    raise result
            logger.info("Worker stopped.")
        except Exception:
            self._started = True
            raise

    # Consumer
    async def _event_consumer_loop(self):
        """
        Transfer ready events using the processing queue's native backpressure.
        """

        logger.info("Event loop started.")

        try:
            while True:
                if self._pending_event is None:
                    self._pending_event = await self._event_pipe.get()

                # Clear ownership only after put succeeds. Cancellation while
                # waiting leaves this event first in line for the next consumer.
                await self._processing_queue.put(self._pending_event)
                self._pending_event = None

        except asyncio.CancelledError:
            logger.info("Event loop cancelled.")

    async def _event_dispatcher_loop(self) -> None:
        """Acquire execution capacity before dequeueing and launching a task."""
        while True:
            await self._dispatch_semaphore.acquire()
            try:
                event = await self._processing_queue.get()
            except BaseException:
                self._dispatch_semaphore.release()
                raise

            try:
                # Observe only events admitted for local dispatch, including
                # events received from a mailbox or written to builtin directly.
                if event.event_name:
                    self._event_registry.record_event(event)
                # Resolve synchronously after processing dequeue, before any task
                # can observe later registration or ordering changes.
                handler_chain = (
                    self._registry.get_handlers_chain_for_event(event.event_name)
                    if event.event_name else []
                )
                if SHOW_EVENT_DISPATCH:
                    event_name_block = event.event_name
                    print(f"\033[38;5;59m{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\033[0m"
                        f" \033[1;38;5;147m[EVENT LOOP]\033[0m"
                        f" \033[38;5;59m│\033[0m"
                        f" \033[38;5;116m{event_name_block}\033[0m"
                    )

                coroutine = self._dispatch_event(event, handler_chain)
                try:
                    task = asyncio.create_task(coroutine)
                except BaseException:
                    coroutine.close()
                    raise
            except Exception as exc:
                # A failed resolution or launch must not strand queue ownership.
                self._event_pipe.task_done()
                self._dispatch_semaphore.release()
                logger.error(
                    f"Dispatch preparation failed: {type(exc).__name__}: "
                    f"{exc}\n{traceback.format_exc()}"
                )
            else:
                self._dispatch_tasks.add(task)
                task.add_done_callback(self._on_dispatch_done)
            finally:
                # Processing queue ownership ends at task creation. Ready queue
                # acknowledgement and dispatch capacity remain with the task.
                self._processing_queue.task_done()

    def _on_dispatch_done(self, task: asyncio.Task) -> None:
        """Release queue ownership even when cancelled before coroutine entry."""
        self._dispatch_tasks.discard(task)
        try:
            self._event_pipe.task_done()
        finally:
            self._dispatch_semaphore.release()

        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    f"Dispatch task failed: {type(error).__name__}: {error}"
                )

    async def _create_background_handler_task(
        self,
        handler_name: str,
        event: ApixEvent,
    ):
        await self._background_handler_semaphore.acquire()

        coroutine = self._run_background_handler(handler_name, event)
        try:
            task = asyncio.create_task(coroutine)
        except BaseException:
            # Release capacity if task creation fails.
            coroutine.close()
            self._background_handler_semaphore.release()
            raise

        self._background_handler_tasks.add(task)
        task.add_done_callback(self._on_background_handler_done)

    async def _run_background_handler(
        self,
        handler_name: str,
        event: ApixEvent,
    ):
        """
        Execute background handler safely.
        """
        # Capacity may have required a wait. Resolve the current entry only
        # now and recheck its current subscription and exclusions.
        handler = self._registry.get_handler(handler_name)
        if handler is not None and self._registry._matches_handler(handler, event.event_name):
            try:
                await handler.execute(event)
            except asyncio.CancelledError:
                await handler.notify_cancelled(event)
                raise

    def _on_background_handler_done(self, task: asyncio.Task) -> None:
        # Also runs when the task is cancelled before its coroutine starts.
        self._background_handler_tasks.discard(task)
        self._background_handler_semaphore.release()

    async def _dispatch_event(
        self,
        event: ApixEvent,
        handler_chain: list[str],
    ) -> ApixEvent | None:
        """
        Dispatch event to registered handlers and notify foreground cancellation.
        """
        logger.debug(
            f"Dispatching event `{event.event_name}` to {len(handler_chain)} handlers."
        )
        try:
            if not event.event_name:
                return None

            if not handler_chain:
                return event

            for handler_name in handler_chain:
                # Earlier handlers may await while subscriptions change.
                handler = self._registry.get_handler(handler_name)
                if handler is None or not self._registry._matches_handler(handler, event.event_name):
                    continue

                if handler.background:
                    await self._create_background_handler_task(handler_name, event)
                else:
                    await handler.execute(event)

            return event

        except asyncio.CancelledError:
            # Notify the entire candidate chain, including handlers whose core
            # already ran or has not started. Never resume normal execution.
            tasks = []
            for handler_name in handler_chain:
                handler = self._registry.get_handler(handler_name)
                if (
                    handler is not None
                    and not handler.background
                    and self._registry._matches_handler(handler, event.event_name)
                ):
                    tasks.append(handler.notify_cancelled(event))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise
        except Exception as e:
            logger.error(
                f"Dispatch failed: "
                f"{type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )


__all__ = ["ApixEventLoop"]

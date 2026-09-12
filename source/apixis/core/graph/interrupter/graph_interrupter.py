import asyncio
from collections.abc import Awaitable
from functools import wraps
from uuid import uuid4

from typing import Any, Callable

from apixis.core.graph.context.manager import get_graph_context
from apixis.core.graph.context.graph_context import GraphContext
from apixis.core.event import (
    ApixEvent,
    ApixEventHandler,
    EVENT_PIPE,
    EventType,
    subscribe,
)
from apixis.core.graph.interrupter.base import Block
from apixis.core.utils.exception import GraphNodeError


InterruptedHandler = Callable[[Block], Awaitable[None]]


async def interrupt(
    *,
    data: Any = None,
    timeout: float | None = None,
    context: GraphContext | None = None
) -> Any:
    """
    Send data and while in graph loop and block the agent graph at the same time.
    Can be called from inside any graph node.

    Args:
        data: Optional chunk data.
        timeout: Optional timeout in seconds for the blocking wait. If None, wait indefinitely.
        context: Optional graph context, if the interruption is not within a :class:`NodeFunction`,
            it is required.

    This method will post a :class:`Block` by event pipe, not by stream writer.
    To receive a :class:`Block` posted by this method, it is required to register a handler
    for event `graph_{namespace}_interrupted` and get block item from event context.
    """

    if context is None:
        try:
            context = get_graph_context()
        except RuntimeError as exc:
            raise RuntimeError(
                "interrupt() is only available while a graph is invoked."
            ) from exc

    if not context.is_active:
        raise RuntimeError(
            "interrupt() is only available while an active graph node "
            "is executed."
        )

    run_id = context.run_id
    namespace = context._context_namespace
    assert run_id is not None
    assert namespace is not None

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    block = Block(
        run_id=run_id,
        block_id=uuid4().hex,
        namespace=namespace,
        with_data=data,
        _future=future,
    )
    await EVENT_PIPE.post_event(
        event_type=EventType.WORKFLOW,
        event_name=f"graph_{namespace}_interrupted",
        context=block,
    )

    try:
        if timeout is None:
            return await block
        timeout_scope = asyncio.timeout(timeout)
        try:
            async with timeout_scope:
                return await block
        except TimeoutError:
            # A hook may fail the block with TimeoutError itself. Only this
            # interruption's own deadline is converted into a None result.
            if not timeout_scope.expired():
                raise
            return None
    except asyncio.CancelledError:
        # External ``Block.cancel()`` aborts the owning graph attempt at its
        # last committed snapshot. The CancelledError is then re-raised to
        # stop the interrupted node immediately, so neither its remaining
        # code nor a downstream route can run. Runtime task cancellation is
        # left to the surrounding graph timeout/cancellation machinery.
        current_task = asyncio.current_task()
        if block.cancelled and (
            current_task is None or current_task.cancelling() == 0
        ):
            context.abort()
        raise


def interrupted_hook(
    namespace: str | None = None,
    *,
    exist_ok: bool = True,
) -> Callable[
    [InterruptedHandler],
    InterruptedHandler,
]:
    """Register a callback receiving the :class:`Block` for a namespace.

    The event runtime dispatches :class:`ApixEvent` objects internally. This
    decorator hides that transport detail and gives application callbacks the
    ``Block`` promised by the public API.

    Usage:
        @interrupted_hook(namespace="agent")
        async def on_interrupted(block: Block):
            ...
    """
    event_name = f"graph_{namespace or '<global>'}_interrupted"

    def decorator(func: InterruptedHandler) -> InterruptedHandler:
        @wraps(func)
        async def dispatch_block(event: ApixEvent) -> None:
            block = event.context
            if not isinstance(block, Block):
                raise TypeError(
                    "Interrupted graph events must carry a Block context."
                )
            await func(block)

        async def on_failure(event: ApixEvent, error: Exception) -> None:
            """Deliver this handler's own failure to its waiting node."""
            block = event.context
            if isinstance(block, Block):
                block.fail(error)

        async def on_has_error(event: ApixEvent) -> None:
            """Deliver upstream failures to the node awaiting this block."""
            block = event.context
            if isinstance(block, Block):
                block.fail(GraphNodeError(
                    "Graph interruption failed in a preceding event handler",
                    errors=list(event.error_stack),
                ))

        async def on_accepted(event: ApixEvent) -> None:
            """Cancel an unhandled block when its event is accepted upstream."""
            block = event.context
            if isinstance(block, Block) and not block.done:
                block.cancel()

        subscribe(
            event_name,
            exist_ok=exist_ok,
        )(ApixEventHandler(
            dispatch_block,
            on_accepted=on_accepted,
            on_has_error=on_has_error,
            on_error=on_failure,
        ))
        return func

    return decorator

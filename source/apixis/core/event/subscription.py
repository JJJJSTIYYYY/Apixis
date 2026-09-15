"""Subscription conveniences backed by the factory-managed handler registry."""

from copy import copy

from apixis.core.event.base import ApixEventHandler, EventHandlerFunc
from apixis.core.event.factory import get_handler_registry


def subscribe(
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
):
    """Register an async handler for one or more event-name patterns.

    Handler names are unique across the process-global registry. The decorated
    function or ApixEventHandler instance is returned unchanged. Validation
    happens in register_handler when the returned decorator is applied.

    Event subscription and filtering use case-sensitive
    :func:`fnmatch.fnmatchcase` semantics. The handler chain is resolved lazily
    from the priority buckets when an event is dequeued or the chain is queried.

    Args:
        event_names:
            One or more event-name patterns to subscribe to. Exact names and
            ``fnmatchcase`` wildcards are both supported, including ``*``,
            ``?``, and character classes such as ``[a-z]``.

            Duplicate patterns are removed while preserving their first
            occurrence. At least one non-empty pattern is required.

            Matching is case-sensitive. For example, ``"Graph.*"`` matches
            ``"Graph.Start"`` but does not match ``"graph.start"``.

        exist_ok:
            If ``True``, replace an existing same-name entry with this handler
            and configuration. Validation failures preserve the old registration.

            If ``False``, a duplicate function name raises
            :class:`EventHandlerAlreadyRegisteredError`.

            Uniqueness is based on the handler name, regardless of subscribed
            patterns or callback identity.

        priority:
            Numeric handler priority. Higher values are dispatched first.
            Handlers with the same priority retain registration order.

            Defaults to ``1`` when ``between_handlers`` is not specified.
            ``priority`` and ``between_handlers`` cannot be supplied together.

        between_handlers:
            Insert the handler relative to active handler names already stored
            in ``priority_buckets``. Boundary names refer to handler function
            names and must already be actively registered.

            Supported forms:

            1. ``(left_handler, right_handler)``

               Insert after ``left_handler`` and immediately before
               ``right_handler``. Existing handlers between the boundaries
               remain before the new handler.

               Example::

                   Existing order:
                       left_handler
                       handler_a
                       handler_b
                       right_handler

                   Registration:
                       between_handlers=(
                           "left_handler",
                           "right_handler",
                       )

                   Result:
                       left_handler
                       handler_a
                       handler_b
                       new_handler
                       right_handler

               The left boundary must already dispatch before the right
               boundary. Reversed boundaries raise ``ValueError``.

            2. ``(None, right_handler)``

               Insert immediately before ``right_handler``.

               Example::

                   Existing:
                       handler_a
                       right_handler
                       handler_b

                   Result:
                       handler_a
                       new_handler
                       right_handler
                       handler_b

            3. ``(left_handler, None)``

               Insert immediately after ``left_handler``.

               Example::

                   Existing:
                       handler_a
                       left_handler
                       handler_b

                   Result:
                       handler_a
                       left_handler
                       new_handler
                       handler_b

            When a right boundary is present, the new handler is inserted into
            the right boundary's priority bucket. With only a left boundary,
            it is inserted into the left boundary's bucket. The handler's own
            ``priority`` metadata remains ``None``.

            ``(None, None)`` is invalid. The two boundaries cannot name the
            same handler, and every non-``None`` boundary must be a non-empty
            string.

        filter_event:
            Optional case-sensitive event-name patterns to exclude after a
            subscription matches. A handler is selected only when the exact
            event name matches at least one ``event_names`` pattern and does
            not match any ``filter_event`` pattern.

            Example::

                @subscribe(
                    "graph.*",
                    filter_event=["graph.internal.*"],
                )
                async def public_graph_handler(event):
                    ...

            The example receives ``"graph.started"`` but not
            ``"graph.internal.snapshot"``.

        stop_when_error:
            If ``True``, skip this handler's core function when the event has
            upstream errors. Error notifications still run. If ``False``, the
            core may run after notification unless the event is accepted.
            Background-handler failures are logged without changing event errors.

        time_out:
            Maximum execution time in seconds for each invoked core or
            notification function. ``None`` preserves the supplied instance's
            timeout (unlimited for a plain function). Values less than or equal
            to zero explicitly disable the timeout.

        background:
            If ``True``, schedule the handler as a background task without
            waiting for it before dispatching subsequent handlers.

    Dispatch rules:
        1. Each event instance is dispatched in its own task.
        2. Higher-priority buckets dispatch before lower-priority buckets.
        3. Handlers in one bucket retain their explicit or registration order.
        4. ``between_handlers`` determines placement when supplied and cannot
           be combined with an explicit priority.
        5. Candidate names and order are fixed when the event is dequeued.
           Each invocation looks up the current handler and rechecks its
           subscription and filters, skipping missing or nonmatching entries.
           Background tasks check again after waiting for capacity. Calls
           already started continue to completion.
        6. Events with different exact names may dispatch concurrently.
        7. Calling :meth:`ApixEvent.accept` skips subsequent core functions,
           while applicable error and acceptance notifications still run.
        8. Subscription and ordering options replace existing metadata.
           Execution options set to None preserve a supplied instance's settings;
           explicit values override them. Notification functions are preserved.

    Examples:
        Register handlers by priority::

            @subscribe("graph.*", priority=10)
            async def validate_graph_event(event):
                ...

            @subscribe("graph.*", priority=1)
            async def persist_graph_event(event):
                ...

        Insert a handler before an existing handler::

            @subscribe(
                "graph.*",
                between_handlers=(None, "persist_graph_event"),
            )
            async def enrich_graph_event(event):
                ...

    Returns:
        A decorator that returns the supplied function or handler instance
        unchanged after successful registration or replacement.

    Raises:
        ValueError:
            If no event-name pattern is supplied; a pattern is empty;
            ``between_handlers`` is malformed; both boundaries are ``None``;
            boundary names are empty or equal; explicit priority is combined
            with boundaries; boundaries are reversed; or priority is not
            finite.

        TypeError:
            If a priority is not numeric or the decorated callback is not
            callable.

        EventHandlerNotRegisteredError:
            If a non-``None`` boundary handler is not actively registered.

        EventHandlerAlreadyRegisteredError:
            If ``exist_ok`` is ``False`` and the function name already exists
            in the handler registry.
    """
    def decorator[HandlerT: EventHandlerFunc | ApixEventHandler](
        func: HandlerT,
    ) -> HandlerT:
        handler_name = func.__name__
        # Stage instance metadata separately so a failed replacement cannot
        # mutate an object that is still registered or being executed.
        entry = (
            copy(func) if isinstance(func, ApixEventHandler)
            else ApixEventHandler(func)
        )
        entry.name = handler_name
        entry.subscribe = list(event_names)
        entry.filter_event = filter_event if filter_event is not None else []
        entry.priority = 1 if priority is None and between_handlers is None else priority
        entry.between_handlers = between_handlers
        # Explicit execution options override the supplied instance's settings.
        # None preserves its setting; non-positive timeouts disable its limit.
        if stop_when_error is not None:
            entry.stop_when_error = stop_when_error
        if time_out is not None:
            entry.time_out = time_out if time_out > 0 else None
        if background is not None:
            entry.background = background
        registry = get_handler_registry()
        registry.register_handler(entry, exist_ok=exist_ok)
        if isinstance(func, ApixEventHandler):
            func.__dict__.update(entry.__dict__)
            registry.registry[handler_name] = func
        return func

    return decorator


def unsubscribe(
    handler_name: str,
    *,
    missing_ok: bool = True,
) -> None:
    """Immediately remove a global handler; optionally reject unknown names."""
    get_handler_registry().unregister_handler(handler_name, missing_ok = missing_ok)


def get_handler(
    handler_name: str,
) -> ApixEventHandler | None:
    return get_handler_registry().get_handler(handler_name)


def get_handler_meta(
    handler_name: str,
) -> dict | None:
    handler = get_handler_registry().get_handler(handler_name)
    if handler is None:
        return None
    return {
        'id': handler.id,
        'name': handler.name,
        'register_order': handler._register_order,
        'subscribe': handler.subscribe,
        'filter_event': handler.filter_event,
        'priority': handler.priority,
        'between_handlers': handler.between_handlers,
        'stop_when_error': handler.stop_when_error,
        'time_out': handler.time_out,
        'background': handler.background,
    }


def is_registered(
    handler_name: str,
) -> bool:
    """Return if a handler is registered."""
    return handler_name in get_handler_registry()


def get_unmatched_subscriptions(handler_name: str) -> list[str]:
    """Return global handler patterns that matched no observed event name."""
    return get_handler_registry().get_unmatched_subscriptions(handler_name)


__all__ = [
    "subscribe", "unsubscribe", "get_handler", "get_handler_meta",
    "is_registered", "get_unmatched_subscriptions",
]

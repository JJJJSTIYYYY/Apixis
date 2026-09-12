"""Shared types, registries, and predefined names for graph execution."""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Any,
    TYPE_CHECKING,
    TypeAlias,
    get_args,
    get_origin,
    get_type_hints,
)

if TYPE_CHECKING:
    from apixis.core.graph.node_graph import NodeGraph


@dataclass(frozen=True, slots=True)
class AutoMerge:
    """Mark an ``Annotated`` state field as auto-increasing.

    This class contains no runtime data. It is metadata collected by
    :class:`GraphContext` and used when a :class:`Command` update is applied.

    When an existing marked field is updated, the graph calls the current
    value's ``__add__`` method with the update value. A field that is not yet
    present in state is initialized directly from the update value.

    Override ``__add__`` to define a custom merge logic.

    Example:
        ``messages: Annotated[list, AutoMerge()]``
    """

    pass


@dataclass(frozen=True, slots=True)
class KeepRef:
    """Mark an ``Annotated`` state field to keep its reference during copying.

    This class contains no runtime data. It is metadata collected by
    :class:`GraphContext` and used when a state copy operation is performed.

    When a marked field is copied, the graph keeps the original field value's
    reference instead of creating a copied object. Other state fields continue
    to follow the normal copy behavior.

    This is useful for fields that represent shared runtime resources or
    mutable objects that should remain synchronized across copied states.

    !!! Warning:
        Fields marked with ``KeepRef`` are not recommended for concurrent use.
        Since copied states share the same object reference, concurrent graph
        executions or parallel node operations may access and mutate the same
        object, causing unexpected side effects or race conditions.
        Additionally, it is also not recommended to update the key marked with
        ``KeepRef`` via Command. **This may lead to unpredictable behavior.**

    Example:
        ``context: Annotated[ContextOrganizer, KeepRef()]``
    """

    pass


@dataclass(frozen=True, slots=True)
class Reset:
    """Explicitly replace a state field during a command update.

    ``Reset`` is primarily used to bypass :class:`AutoMerge` for one
    update. The graph unwraps it before storing the value, so the wrapper never
    becomes part of the resulting state.

    Example:
        ``Command(update={"messages": Reset([])})``
    """

    value: Any


def _copy_state(
    state: dict,
    keep_ref_keys: frozenset[str],
) -> dict:
    """Copy graph state while preserving fields marked with ``KeepRef``.

    Args:
        state: State mapping to copy.
        keep_ref_keys: Fields whose values must retain object identity.

    Raises:
        TypeError: If ``state`` is not a dictionary.
    """
    if not isinstance(state, dict):
        raise TypeError("Graph state must be a dict.")

    if not keep_ref_keys:
        return copy.deepcopy(state)

    keep_refs = {
        key: state[key]
        for key in keep_ref_keys
        if key in state
    }

    # Exclude kept fields before deepcopy so resource-like values do not need
    # to support copying. Rebuilding in original key order also preserves the
    # alias behavior of ordinary fields.
    copied_values = copy.deepcopy(
        {
            key: value
            for key, value in state.items()
            if key not in keep_refs
        }
    )
    return {
        key: (
            keep_refs[key]
            if key in keep_refs
            else copied_values[key]
        )
        for key in state
    }


def get_auto_merge_keys(
    state_schema: type | None,
) -> frozenset[str]:
    """Return fields marked with :class:`AutoMerge` in a state schema.

    ``state_schema`` is normally a ``TypedDict`` class. Regular annotated
    classes are also accepted because only their resolved type hints are
    inspected.

    Both ``AutoMerge`` and ``AutoMerge()`` metadata forms are supported.

    Args:
        state_schema:
            State schema whose ``Annotated`` metadata should be inspected.
            ``None`` disables auto-increasing updates.

    Raises:
        TypeError:
            If ``state_schema`` is not a class.
        NameError:
            If the schema contains an unresolved forward reference.
    """
    if state_schema is None:
        return frozenset()

    if not isinstance(state_schema, type):
        raise TypeError(
            "`state_schema` must be a class or None, "
            f"got {type(state_schema).__name__}."
        )

    type_hints = get_type_hints(
        state_schema,
        include_extras=True,
    )

    return frozenset(
        key
        for key, annotation in type_hints.items()
        if (
            get_origin(annotation) is Annotated
            and any(
                marker is AutoMerge
                or isinstance(marker, AutoMerge)
                for marker in get_args(annotation)[1:]
            )
        )
    )


def get_keep_ref_keys(
    state_schema: type | None,
) -> frozenset[str]:
    """Return fields marked with :class:`KeepRef` in a state schema.

    ``state_schema`` is normally a ``TypedDict`` class. Regular annotated
    classes are also accepted because only their resolved type hints are
    inspected.

    Both ``KeepRef`` and ``KeepRef()`` metadata forms are supported.

    Args:
        state_schema:
            State schema whose ``Annotated`` metadata should be inspected.
            ``None`` returns an empty set.

    Raises:
        TypeError:
            If ``state_schema`` is not a class.
        NameError:
            If the schema contains an unresolved forward reference.
    """
    if state_schema is None:
        return frozenset()

    if not isinstance(state_schema, type):
        raise TypeError(
            "`state_schema` must be a class or None, "
            f"got {type(state_schema).__name__}."
        )

    type_hints = get_type_hints(
        state_schema,
        include_extras=True,
    )

    return frozenset(
        key
        for key, annotation in type_hints.items()
        if (
            get_origin(annotation) is Annotated
            and any(
                marker is KeepRef
                or isinstance(marker, KeepRef)
                for marker in get_args(annotation)[1:]
            )
        )
    )


START = "__start__"
"""Predefined node name that begins every graph invocation."""

END = "__end__"
"""Predefined node name that completes every graph invocation."""


GRAPH_DISPATCH = "__graph_dispatch__"
"""Base name qualified by get_graph_dispatch_name for graph events and handlers."""

_namespace_graphs: dict[str, NodeGraph] = {}
"""Compiled graph indexed by its exclusive listener namespace."""

namespace_set = _namespace_graphs.keys()
"""Namespaces currently owned by compiled graphs."""


def acquire_namespace(
    graph: NodeGraph,
    *,
    replace_existed: bool = False,
) -> NodeGraph:
    """Validate and acquire the graph's exclusive namespace.

    An occupied namespace is rejected by default. With ``replace_existed=True``, its
    current graph is decomposed before ownership is transferred. Glob characters
    are rejected before changing ownership. The caller must release ownership
    if subsequent listener registration fails.
    """
    namespace = graph.namespace
    if any(character in namespace for character in "*?[]"):
        raise ValueError(
            f"Graph namespace `{namespace}` must not contain glob characters (*?[])."
        )
    if namespace in namespace_set:
        if not replace_existed:
            raise ValueError(
                f"Graph namespace `{namespace}` is already in use."
            )
        if _namespace_graphs[namespace] is graph:
            return graph
        _namespace_graphs[namespace].decompose()

    _namespace_graphs[namespace] = graph
    return graph


def release_namespace(graph: NodeGraph) -> None:
    """Release ``graph`` only when it still owns its namespace."""
    namespace = graph.namespace
    if _namespace_graphs.get(namespace) is graph:
        _namespace_graphs.pop(namespace)


@dataclass(slots=True)
class Command:
    """A node result that updates state and optionally chooses the next node.

    Attributes:
        update:
            Values merged into the state carried by the next event.
            Wrapping a value in :class:`Reset` explicitly replaces that
            field even when its state annotation contains
            :class:`AutoMerge`.
        goto:
            One or more next node names. A list schedules its nodes in one
            concurrent batch and also defines their deterministic result
            application order. ``None`` permits a manager-defined default
            transition, while an empty list ends the graph immediately.
    """

    update: dict[str, Any] = field(default_factory=dict)
    goto: str | list[str] | None = None


NodeResult: TypeAlias = dict[str, Any] | Command
"""A state update or one command returned by a regular graph node."""

NodeFunction: TypeAlias = (
    Callable[[dict[str, Any]], NodeResult]
    | Callable[[dict[str, Any]], Awaitable[NodeResult]]
)
"""A synchronous or asynchronous callable that receives graph state."""


def get_graph_dispatch_name(
    namespace: str | None = None,
    missing_ok: bool = True,
) -> str:
    """Return the event and handler name for graph dispatch.

    Use the returned name with ``subscribe`` and as the graph handler boundary
    in ``between_handlers``. No node name is needed.

    Args:
        namespace: Graph namespace. ``None`` and an empty string select
            ``<global>``. ``*`` produces a subscription pattern matching
            every graph, including the global graph. A wildcard pattern
            cannot identify a single handler for ``between_handlers``.
        missing_ok: If ``False``, require a concrete namespace to be owned by
            a compiled graph. The wildcard namespace ``*`` is always accepted.
    """
    namespace = namespace or "<global>"

    if not missing_ok and namespace != "*" and namespace not in namespace_set:
        raise KeyError(f"Namespace `{namespace}` not found in current namespace set.")

    return f"{GRAPH_DISPATCH}_{namespace}"

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
    :class:`NodeGraph` and used when a :class:`Command` update is applied.

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
    :class:`NodeGraph` and used when a state copy operation is performed.

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

    keep_refs = {key: state[key] for key in keep_ref_keys if key in state}

    # Exclude kept fields before deepcopy so resource-like values do not need
    # to support copying. Rebuilding in original key order also preserves the
    # alias behavior of ordinary fields.
    copied_values = copy.deepcopy(
        {key: value for key, value in state.items() if key not in keep_refs}
    )
    return {
        key: (keep_refs[key] if key in keep_refs else copied_values[key])
        for key in state
    }


def parse_state_schema(
    state_schema: type | None,
) -> tuple[frozenset[str], frozenset[str]]:
    """Resolve annotations once for the compiled graph's two state policies.

    TypedDict and regular annotated classes are accepted. Both marker classes
    and instances are supported. None disables both policies. Invalid classes
    and unresolved forward references fail during graph compilation.
    """
    if state_schema is None:
        return frozenset(), frozenset()
    if not isinstance(state_schema, type):
        raise TypeError(
            "`state_schema` must be a class or None, "
            f"got {type(state_schema).__name__}."
        )
    merge: set[str] = set()
    keep: set[str] = set()
    for key, annotation in get_type_hints(state_schema, include_extras=True).items():
        if get_origin(annotation) is Annotated:
            for marker in get_args(annotation)[1:]:
                if marker is AutoMerge or isinstance(marker, AutoMerge):
                    merge.add(key)
                if marker is KeepRef or isinstance(marker, KeepRef):
                    keep.add(key)
    return frozenset(merge), frozenset(keep)


START = "__start__"
"""Predefined node name that begins every graph invocation."""

END = "__end__"
"""Predefined node name that completes every graph invocation."""


GRAPH_DISPATCH = "__graph_dispatch__"
"""Base name qualified by get_graph_dispatch_name for graph events and handlers."""

GLOBALNS = "<global>"
"""Explicit namespace for graph events and handlers in the global domain."""

_namespace_graphs: dict[str, NodeGraph] = {}
"""Compiled graph indexed by its exclusive listener namespace."""

namespace_set = _namespace_graphs.keys()
"""Namespaces currently owned by compiled graphs."""


def validate_namespace(namespace: str) -> None:
    """Validate a graph registration name without changing ownership."""
    if any(character in namespace for character in "*?[]"):
        raise ValueError(
            f"Graph namespace `{namespace}` must not contain glob characters (*?[])."
        )


def acquire_namespace(
    graph: NodeGraph,
    *,
    replace_existed: bool = False,
) -> NodeGraph:
    """Claim a namespace, forcefully retiring its previous graph if requested.

    Replacement completes synchronously. Listener registration remains the
    caller's responsibility; later failures do not restore the retired graph.
    """
    validate_namespace(graph.namespace)
    owner = _namespace_graphs.get(graph.namespace)
    if owner is not None and owner is not graph:
        if not replace_existed:
            raise ValueError(f"Graph namespace `{graph.namespace}` is already in use.")
        release_namespace(owner)
    _namespace_graphs[graph.namespace] = graph
    return graph


def get_graph_namespace(graph_id: str) -> str:
    """Resolve a live graph ID from the existing namespace registry."""
    for namespace, graph in _namespace_graphs.items():
        if graph.graph_id == graph_id:
            return namespace
    raise RuntimeError("The context's graph is no longer registered.")


def release_namespace(
    graph: NodeGraph,
    *,
    decompose_immediately: bool = True,
) -> None:
    """Optionally retire the graph, then release only its own registration.

    Repeated cleanup and cleanup of a replaced graph are harmless. Decomposition
    calls back with decompose_immediately=False to avoid recursive teardown.
    """
    if decompose_immediately:
        graph.decompose(force=True)
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
    namespace_or_graph: str | NodeGraph | None = None,
    missing_ok: bool = True,
) -> str:
    """Return the event and handler name for graph dispatch.

    Use the returned name with ``subscribe`` and as the graph handler boundary
    in ``between_handlers``. No node name is needed.

    Args:
        namespace_or_graph: Graph namespace or graph instance. ``None`` and an empty string select
            ``<global>``. ``*`` produces a subscription pattern matching
            every graph, including the global graph. A wildcard pattern
            cannot identify a single handler for ``between_handlers``.
        missing_ok: If ``False``, require a concrete namespace to be owned by
            a compiled graph. The wildcard namespace ``*`` is always accepted.
    """
    if namespace_or_graph and not isinstance(namespace_or_graph, str):
        namespace = namespace_or_graph.namespace or GLOBALNS
    else:
        namespace = namespace_or_graph or GLOBALNS

    if not missing_ok and namespace != "*" and namespace not in namespace_set:
        raise KeyError(f"Namespace `{namespace}` not found in current namespace set.")

    return f"{GRAPH_DISPATCH}_{namespace}"

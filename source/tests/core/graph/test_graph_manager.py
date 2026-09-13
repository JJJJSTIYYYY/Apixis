"""Unit tests for graph construction and transition validation."""

import math

import pytest

from apixis.core.event import APIX_HANDLER_REGISTRY
from apixis.core.graph import (
    END,
    GRAPH_DISPATCH,
    START,
    GraphManager,
    get_graph_dispatch_name,
    namespace_set,
)
from apixis.core.graph.base import (
    _namespace_graphs,
    release_namespace,
)


def source(state):
    return {}


def target(state):
    return {}


def test_add_node_and_add_nodes_are_fluent():
    """Builder registration methods return the same manager instance."""
    manager = GraphManager()

    assert manager.add_node(source) is manager
    assert manager.add_nodes([target]) is manager


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [
        (None, None),
        (0, None),
        (-1, None),
        (1, 1.0),
        (1.5, 1.5),
    ],
)
def test_add_node_stores_normalised_timeout(timeout, expected):
    """Node owns its timeout and non-positive values mean unlimited."""
    manager = GraphManager().add_node(source, timeout=timeout)

    assert manager._nodes["source"].timeout == expected


@pytest.mark.parametrize("timeout", [True, "1", object()])
def test_add_node_rejects_non_numeric_timeout_without_registering_node(timeout):
    """Invalid timeout types leave the manager unchanged."""
    manager = GraphManager()

    with pytest.raises(TypeError, match="timeout must be a number or None"):
        manager.add_node(source, timeout=timeout)

    assert manager.has_node("source") is False


@pytest.mark.parametrize("timeout", [math.inf, -math.inf, math.nan])
def test_add_node_rejects_non_finite_timeout_without_registering_node(timeout):
    """NaN and infinity cannot represent executable deadlines."""
    manager = GraphManager()

    with pytest.raises(ValueError, match="timeout must be finite"):
        manager.add_node(source, timeout=timeout)

    assert manager.has_node("source") is False


@pytest.mark.parametrize("reserved_name", [START, END])
def test_reserved_node_names_are_rejected(reserved_name):
    """User nodes cannot replace the predefined START and END nodes."""
    with pytest.raises(ValueError, match="reserved graph node name"):
        GraphManager().add_node(source, reserved_name)


def test_duplicate_node_name_is_rejected():
    """Every user node name must be unique."""
    manager = GraphManager().add_node(source)

    with pytest.raises(ValueError, match="already registered"):
        manager.add_node(target, "source")


@pytest.mark.parametrize(
    ("left", "right", "message"),
    [
        ("missing", END, "has not been added"),
        (START, "missing", "has not been added"),
        (END, START, "cannot have an outgoing transition"),
    ],
)
def test_edge_endpoints_must_exist_and_end_cannot_be_a_source(left, right, message):
    """Edges validate both endpoints and the terminal END constraint."""
    with pytest.raises(ValueError, match=message):
        GraphManager().add_edge(left, right)


def test_only_one_manager_transition_is_allowed_per_source():
    """A source cannot have two direct or generated outgoing transitions."""
    manager = GraphManager().add_nodes([source, target]).add_edge(START, "source")

    with pytest.raises(ValueError, match="already has an outgoing transition"):
        manager.add_edge(START, "target")


def test_condition_must_be_callable():
    """Conditional edges reject non-callable predicates."""
    manager = GraphManager().add_nodes([source, target])

    with pytest.raises(TypeError, match="condition.*callable"):
        manager.add_edge("source", "target", condition=True)


def test_generated_condition_name_avoids_user_node_collision():
    """Generated helper nodes receive a suffix when their base name exists."""

    def predicate(state):
        return True

    manager = (
        GraphManager()
        .add_nodes([source, target])
        .add_node(lambda state: {}, "__condition__source__predicate")
    )

    manager.add_edge("source", "target", predicate)

    assert "__condition__source__predicate_2" in manager._nodes


def test_router_requires_at_least_one_target():
    """A router without declared destinations cannot be constructed."""
    manager = GraphManager().add_node(source)

    with pytest.raises(ValueError, match="at least one target"):
        manager.add_router("source", [], lambda state: END)


def test_router_targets_must_exist():
    """Every declared router destination must be a known node or END."""
    manager = GraphManager().add_node(source)

    with pytest.raises(ValueError, match="has not been added"):
        manager.add_router("source", ["missing"], lambda state: "missing")


def test_router_must_be_callable():
    """Router definitions reject non-callable selectors."""
    manager = GraphManager().add_nodes([source, target])

    with pytest.raises(TypeError, match="router.*NodeFunction"):
        manager.add_router("source", ["target"], router="target")


def test_compile_requires_start_transition():
    """A compiled graph must have an entry transition from START."""
    with pytest.raises(ValueError, match="outgoing transition from `START`"):
        GraphManager().add_node(source).compile_graph()


def test_compile_retains_timeout_on_node():
    """The compiled graph reads timeout policy from its node."""
    graph = (
        GraphManager()
        .add_node(source, timeout=2.5)
        .add_edge(START, "source")
        .compile_graph()
    )

    assert graph._nodes["source"].timeout == 2.5


@pytest.mark.parametrize(
    ("using_namespace", "expected"),
    [
        (None, "<global>"),
        ("", "<global>"),
        ("<global>", "<global>"),
        ("agent-runtime", "agent-runtime"),
    ],
)
def test_compile_forwards_listener_namespace(using_namespace, expected):
    """GraphManager exposes NodeGraph's listener namespace selection."""
    graph = (
        GraphManager()
        .add_node(source)
        .add_edge(START, "source")
        .compile_graph(using_namespace=using_namespace)
    )

    assert graph.namespace == expected
    assert expected in namespace_set
    assert _namespace_graphs[expected] is graph


def test_compile_rejects_occupied_namespace_by_default():
    """An existing compiled graph is retained when replacement is disabled."""
    first_graph = (
        GraphManager()
        .add_node(source)
        .add_edge(START, "source")
        .compile_graph(using_namespace="occupied")
    )

    with pytest.raises(ValueError, match="already in use"):
        (
            GraphManager()
            .add_node(target)
            .add_edge(START, "target")
            .compile_graph(using_namespace="occupied")
        )

    assert first_graph._decomposed is False
    assert _namespace_graphs["occupied"] is first_graph


@pytest.mark.parametrize("namespace", [None, "", "<global>", "replaceable"])
def test_compile_exist_ok_decomposes_and_replaces_original_graph(namespace):
    """Replacement unregisters the old listeners before installing new ones."""
    first_graph = (
        GraphManager()
        .add_node(source, "shared")
        .add_edge(START, "shared")
        .compile_graph(using_namespace=namespace)
    )
    first_callbacks = {
        APIX_HANDLER_REGISTRY.get_handler(handler_name).core_func
        for handler_name in APIX_HANDLER_REGISTRY.get_handlers_chain_for_event(
            first_graph.dispatch_name
        )
    }

    replacement = (
        GraphManager()
        .add_node(target, "shared")
        .add_edge(START, "shared")
        .compile_graph(
            using_namespace=namespace,
            exist_ok=True,
        )
    )
    replacement_callbacks = {
        APIX_HANDLER_REGISTRY.get_handler(handler_name).core_func
        for handler_name in APIX_HANDLER_REGISTRY.get_handlers_chain_for_event(
            replacement.dispatch_name
        )
    }

    assert first_graph._decomposed is True
    assert first_graph._handlers == {}
    assert first_callbacks.isdisjoint(replacement_callbacks)
    assert len(replacement_callbacks) == 1
    assert namespace_set == {namespace or "<global>"}
    assert _namespace_graphs[namespace or "<global>"] is replacement

    release_namespace(first_graph)
    assert namespace_set == {namespace or "<global>"}
    assert _namespace_graphs[namespace or "<global>"] is replacement


def test_decompose_releases_namespace_by_contextmanager():
    graph = (
        GraphManager()
        .add_node(source)
        .add_edge(START, "source")
        .compile_graph(using_namespace="reusable")
    )

    assert "reusable" in namespace_set
    assert "reusable" in _namespace_graphs

    with graph:
        pass

    assert "reusable" not in namespace_set
    assert "reusable" not in _namespace_graphs
    assert True == graph._decomposed


def test_decompose_releases_namespace_for_later_compile():
    """Direct decomposition keeps GraphManager's global index synchronized."""
    first_graph = (
        GraphManager()
        .add_node(source)
        .add_edge(START, "source")
        .compile_graph(using_namespace="reusable")
    )

    assert "reusable" in namespace_set
    assert "reusable" in _namespace_graphs

    first_graph.decompose()

    assert "reusable" not in namespace_set
    assert "reusable" not in _namespace_graphs

    replacement = (
        GraphManager()
        .add_node(target)
        .add_edge(START, "target")
        .compile_graph(using_namespace="reusable")
    )
    assert _namespace_graphs["reusable"] is replacement


def test_none_and_empty_string_share_global_namespace():
    """Both global namespace spellings participate in one uniqueness check."""
    (
        GraphManager()
        .add_node(source)
        .add_edge(START, "source")
        .compile_graph(using_namespace=None)
    )

    with pytest.raises(ValueError, match="<global>"):
        (
            GraphManager()
            .add_node(target)
            .add_edge(START, "target")
            .compile_graph(using_namespace="")
        )


@pytest.mark.parametrize("namespace", ["*", "agent?", "agent[ab]", "agent[", "agent]"])
@pytest.mark.parametrize("exist_ok", [False, True])
def test_compile_rejects_glob_namespace_without_changing_registry(namespace, exist_ok):
    """Invalid names cannot acquire ownership or replace existing listeners."""
    manager = GraphManager().add_node(source).add_edge(START, "source")
    original = manager.compile_graph()
    handlers = dict(APIX_HANDLER_REGISTRY.registry)

    with pytest.raises(ValueError, match="glob characters"):
        manager.compile_graph(using_namespace=namespace, exist_ok=exist_ok)

    assert _namespace_graphs == {"<global>": original}
    assert APIX_HANDLER_REGISTRY.registry == handlers
    assert original._decomposed is False


@pytest.mark.parametrize("namespace", [None, "", "<global>"])
def test_global_event_name_and_ownership_check_use_canonical_namespace(namespace):
    """Global aliases share event naming and the strict ownership check."""
    event_name = f"{GRAPH_DISPATCH}_<global>"
    assert get_graph_dispatch_name(namespace) == event_name
    with pytest.raises(KeyError, match="<global>"):
        get_graph_dispatch_name(namespace, missing_ok=False)

    graph = GraphManager().add_edge(START, END).compile_graph()
    assert (
        get_graph_dispatch_name(
            namespace,
            missing_ok=False,
        )
        == graph.dispatch_name
        == event_name
    )

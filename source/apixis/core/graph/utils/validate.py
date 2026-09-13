from collections.abc import Mapping

from apixis.core.graph.base import END, START
from apixis.core.graph.node import BaseNode


def validate_graph_definition(
    nodes: Mapping[str, BaseNode],
    gotos: Mapping[str, str],
) -> None:
    """Validate node registrations and default graph transitions.

    Node mapping keys define graph-local names independently of BaseNode.name.
    START must have an outgoing transition and cannot be a transition target.
    END may be a transition target but cannot have an outgoing transition.
    Ordinary nodes may omit outgoing transitions or participate in cycles.

    The supplied mappings and node instances are never modified.

    Raises:
        TypeError: If mappings, node names, nodes, or endpoints have invalid types.
        ValueError: If names are empty, reserved, missing, or used incorrectly.
    """
    if not isinstance(nodes, Mapping):
        raise TypeError("nodes must be a mapping.")
    if not isinstance(gotos, Mapping):
        raise TypeError("gotos must be a mapping.")

    for name, node in nodes.items():
        if not isinstance(name, str):
            raise TypeError("Node names must be strings.")
        if not name:
            raise ValueError("Node names must not be empty.")
        if name in (START, END):
            raise ValueError(f"Node name {name!r} is reserved.")
        if not isinstance(node, BaseNode):
            raise TypeError(f"Node {name!r} must be a BaseNode instance.")

    for source, target in gotos.items():
        if not isinstance(source, str):
            raise TypeError("Transition sources must be strings.")
        if not isinstance(target, str):
            raise TypeError(
                f"Transition target for {source!r} must be a string."
            )

        if source == END:
            raise ValueError("END cannot have an outgoing transition.")
        if source != START and source not in nodes:
            raise ValueError(f"Unknown transition source {source!r}.")

        if target == START:
            raise ValueError("START cannot be a transition target.")
        if target != END and target not in nodes:
            raise ValueError(f"Unknown transition target {target!r}.")

    if START not in gotos:
        raise ValueError("A graph must define an outgoing transition from START.")
"""End-to-end tests for core node extensions inside the compiled graph runtime."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph import (
    AutoMerge,
    START,
    BaseNode,
    Command,
    GraphManager,
    NodeGraph,
    ParallelNode,
)


pytestmark = pytest.mark.asyncio(loop_scope="session")


class WorkflowState(TypedDict, total=False):
    """State schema used by the complete custom-node graph."""

    audit: Annotated[list[str], AutoMerge()]
    winner: str
    observed: dict
    route: str


class CommandBatchNode(BaseNode):
    """User-defined node that supplies an ordered batch through the core API."""

    def __init__(self, commands: list[Command], name: str = "batch") -> None:
        self.name = name
        self.commands = commands

    async def execute(self, state: dict) -> list[Command]:
        return self.commands


async def test_concurrent_branches_are_applied_in_declaration_order():
    """GraphManager compiles ParallelNode into one ordered state flow."""
    second_started = asyncio.Event()
    completion_order: list[str] = []

    async def first(state: dict) -> Command:
        await second_started.wait()
        completion_order.append("first")
        return Command(update={"audit": ["first"], "winner": "first"})

    async def second(state: dict) -> Command:
        completion_order.append("second")
        second_started.set()
        return Command(update={"audit": ["second"], "winner": "second"})

    def observe(state: dict) -> dict:
        return {"observed": {"audit": state["audit"], "winner": state["winner"]}}

    parallel_node = ParallelNode([first, second])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([parallel_node, observe])
        .add_edge(START, parallel_node.name)
        .add_edge(parallel_node.name, "observe")
        .compile_graph()
    )

    assert isinstance(graph, NodeGraph)
    result = await asyncio.wait_for(
        graph.invoke({"audit": [], "winner": "initial"}), timeout=1
    )

    assert completion_order == ["second", "first"]
    assert result["audit"] == ["first", "second"]
    assert result["winner"] == "second"
    assert result["observed"] == {"audit": ["first", "second"], "winner": "second"}


async def test_custom_command_goto_overrides_manager_default_edge():
    """A custom node's route is honored after it returns its command list."""
    def fallback(state: dict) -> dict:
        return {"route": "fallback"}

    def selected(state: dict) -> dict:
        return {"route": "selected"}

    batch = CommandBatchNode([
        Command(update={"audit": ["selected"]}, goto="selected"),
    ])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([batch, fallback, selected])
        .add_edge(START, batch.name)
        .add_edge(batch.name, "fallback")
        .compile_graph()
    )

    result = await graph.invoke({"audit": []})

    assert result["route"] == "selected"
    assert result["audit"] == ["selected"]


async def test_empty_command_list_continues_along_default_edge():
    """A custom node's empty Command list acts as a graph no-op."""
    def after_batch(state: dict) -> dict:
        return {"route": "after-batch"}

    batch = CommandBatchNode([])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([batch, after_batch])
        .add_edge(START, batch.name)
        .add_edge(batch.name, "after_batch")
        .compile_graph()
    )

    result = await graph.invoke({"audit": ["initial"]})

    assert result == {"audit": ["initial"], "route": "after-batch"}


async def test_branch_exception_propagates_and_stops_downstream_node():
    """Branch failures complete NodeGraph.invoke with the original exception."""
    downstream_calls: list[str] = []

    async def explode(state: dict) -> dict:
        await asyncio.sleep(0)
        raise RuntimeError("branch exploded")

    def downstream(state: dict) -> dict:
        downstream_calls.append("called")
        return {"route": "downstream"}

    parallel_node = ParallelNode([explode])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([parallel_node, downstream])
        .add_edge(START, parallel_node.name)
        .add_edge(parallel_node.name, "downstream")
        .compile_graph()
    )

    with pytest.raises(RuntimeError, match="branch exploded"):
        await graph.invoke({})

    assert downstream_calls == []


async def test_graph_timeout_cancels_running_parallel_node():
    """GraphManager timeout applies to specialised core node execution."""
    cancelled = asyncio.Event()

    async def slow_branch(state: dict) -> dict:
        try:
            await asyncio.sleep(60)
            return {}
        except asyncio.CancelledError:
            cancelled.set()
            raise

    parallel_node = ParallelNode([slow_branch])
    graph = (
        GraphManager(WorkflowState)
        .add_node(parallel_node, timeout=0.02)
        .add_edge(START, parallel_node.name)
        .compile_graph()
    )

    with pytest.raises(
        TimeoutError,
        match=r"Graph node `parallel` timed out after 0.02 seconds",
    ):
        await graph.invoke({})

    assert cancelled.is_set()

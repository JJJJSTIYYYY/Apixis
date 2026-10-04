"""Build and concurrently run many multi-node graphs through public APIs."""

import asyncio
from math import ceil
from statistics import mean
from time import perf_counter
from typing import Annotated, TypedDict

import pytest

from apixis.core import (
    AutoMerge, Command, GraphManager, get_event_loop,
)


pytestmark = pytest.mark.performance


class WorkloadState(TypedDict):
    run_index: int
    visits: Annotated[list[tuple[int, str]], AutoMerge()]


def _build_graph(topology, nodes, width):
    """Register fresh node objects and compile a fresh namespace each time."""
    names = [f"node_{index}" for index in range(nodes)]
    step_width = 1 if topology == "chain" else width
    layers = [names[index:index + step_width] for index in range(0, nodes, step_width)]
    manager = GraphManager(WorkloadState)

    def make_node(name, target):
        def work(state):
            # Return immediately; inspect visit markers after timing finishes.
            return Command(
                update={"visits": [(state["run_index"], name)]},
                goto=target,
            )
        return work

    for index, layer in enumerate(layers):
        next_layer = layers[index + 1] if index + 1 < len(layers) else []
        target = (next_layer[0] if next_layer else None) if topology == "chain" else next_layer
        for name in layer:
            manager.add_node(make_node(name, target), name)
    entry = layers[0][0] if topology == "chain" else layers[0]
    return manager.compile_graph(entry).set_max_steps(len(layers))


def _percentile(values, percentile):
    """Use the nearest-rank definition, including for small smoke workloads."""
    ordered = sorted(values)
    return ordered[max(0, ceil(len(ordered) * percentile) - 1)] * 1000


async def _run_round(ownership, topology, settings, wait_for_dispatch):
    """Time construction, context preparation, task creation, and all execution."""
    count, nodes = settings["count"], settings["nodes"]
    graphs = []
    contexts = []
    results = [None] * count
    latencies = [0.0] * count
    ready = 0
    all_ready, start_gate = asyncio.Event(), asyncio.Event()

    async def invoke(index, graph, context):
        nonlocal ready
        ready += 1
        if ready == count:
            all_ready.set()
        await start_gate.wait()
        results[index] = await graph.invoke(graph_context=context)
        # The common clock deliberately includes all preceding construction.
        latencies[index] = perf_counter() - started

    started = perf_counter()
    try:
        # Synchronous construction cannot be interrupted by asyncio.timeout.
        # Explicit checks include it in the deadline as well as the metrics.
        deadline = asyncio.get_running_loop().time() + settings["timeout"]
        async with asyncio.timeout_at(deadline):
            for _ in range(count if ownership == "independent" else 1):
                graphs.append(_build_graph(topology, nodes, settings["width"]))
                if perf_counter() - started >= settings["timeout"]:
                    raise TimeoutError("Graph construction exceeded the round deadline")
            built = perf_counter()
            for index in range(count):
                graph = graphs[index] if ownership == "independent" else graphs[0]
                contexts.append(graph.create_context({"run_index": index, "visits": []}))
            async with asyncio.TaskGroup() as group:
                for index, context in enumerate(contexts):
                    graph = graphs[index] if ownership == "independent" else graphs[0]
                    group.create_task(invoke(index, graph, context))
                # Every caller is waiting before the entire cohort is released.
                await all_ready.wait()
                released = perf_counter()
                start_gate.set()
            finished = perf_counter()

        # Result assertions and worker draining are outside the measured time.
        await wait_for_dispatch(get_event_loop())
        expected_steps = nodes if topology == "chain" else ceil(nodes / settings["width"])
        for index, (result, context) in enumerate(zip(results, contexts, strict=True)):
            expected = {"run_index": index, "visits": [(index, f"node_{n}") for n in range(nodes)]}
            assert result == expected
            assert context.state == expected
            assert context.status == "finished"
            assert context.steps == expected_steps
            assert len(context.context_snapshot) == expected_steps
            graph = graphs[index] if ownership == "independent" else graphs[0]
            assert context.graph_id == graph.graph_id
        assert len({context.run_id for context in contexts}) == count
        assert len({graph.namespace for graph in graphs}) == len(graphs)
        node_executions = sum(len(result["visits"]) for result in results)
        assert node_executions == count * nodes
        total = finished - started
        return {
            "total_seconds": total,
            "build_seconds": built - started,
            "prepare_seconds": released - built,
            "execute_seconds": finished - released,
            "completed_invocations": count,
            "node_executions": node_executions,
            "invocations_per_second": count / total,
            "nodes_per_second": node_executions / total,
            "latency_mean_ms": mean(latencies) * 1000,
            "latency_p50_ms": _percentile(latencies, 0.50),
            "latency_p95_ms": _percentile(latencies, 0.95),
            "latency_p99_ms": _percentile(latencies, 0.99),
        }
    finally:
        for graph in graphs:
            graph.decompose()


@pytest.mark.parametrize("ownership", ["independent", "shared"])
@pytest.mark.parametrize("topology", ["chain", "parallel"])
async def test_concurrent_multi_node_graphs(
    ownership, topology, graph_settings, report_graph_metrics, wait_for_dispatch,
):
    """Validate every run; compare timings without hardware-specific thresholds."""
    # Warm up runtime code paths, then rebuild every graph in measured rounds.
    # This preserves construction costs and uses fresh namespace routing caches.
    await _run_round(ownership, topology, graph_settings, wait_for_dispatch)
    rounds = []
    for _ in range(graph_settings["repeats"]):
        rounds.append(await _run_round(ownership, topology, graph_settings, wait_for_dispatch))
    report_graph_metrics({
        "ownership": ownership,
        "topology": topology,
        "graph_instances": graph_settings["count"] if ownership == "independent" else 1,
        "invocations": graph_settings["count"],
        "nodes_per_invocation": graph_settings["nodes"],
        "width": 1 if topology == "chain" else min(graph_settings["width"], graph_settings["nodes"]),
        "warmup_rounds": 1,
        "measured_rounds": graph_settings["repeats"],
        "rounds": rounds,
    })

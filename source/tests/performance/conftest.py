"""Performance settings, quiet logging, and reproducible metric output."""

import importlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import pytest

from apixis.core.config.core_config import EVENT_LOOP_BACKPRESSURE, EVENT_PIPE_MAX_LEN


@pytest.fixture(params=[
    pytest.param((1000, 16), id="1000-invocations-16-nodes"),
    pytest.param((3000, 32), id="3000-invocations-32-nodes"),
])
def graph_settings(request):
    """Run both standard and stress workloads; allow small CI overrides."""
    count, nodes = request.param
    settings = {
        key: request.config.getoption(f"--graph-{key}")
        for key in ("count", "nodes", "width", "repeats", "timeout")
    }
    settings["count"] = settings["count"] if settings["count"] is not None else count
    settings["nodes"] = settings["nodes"] if settings["nodes"] is not None else nodes
    return settings


@pytest.fixture(autouse=True)
def quiet_runtime_logs(monkeypatch):
    """Keep console output out of measurements while preserving error logging."""
    logger_module = importlib.import_module("apixis.core.utils.logger")
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(logger_module, "DEBUG_LEVEL", "ERROR")
    monkeypatch.setattr(loop_module, "SHOW_EVENT_DISPATCH", False)


@pytest.fixture
def report_graph_metrics(request):
    reports = getattr(request.config, "_graph_metrics", None)
    if reports is None:
        reports = request.config._graph_metrics = []
    return reports.append


def _performance_table(reports):
    """Render the current run's measured medians as a standalone table."""
    lines = [
        "# 性能结果 \n\n"
        "| 场景 | 图实例 | 并发调用 | 每次节点数 | 事件背压数 | 总耗时（秒） | 建图（秒） | 节点/秒 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for report in reports:
        rounds = report["rounds"]
        lines.append(
            f"| {report['ownership']}/{report['topology']} "
            f"| {report['graph_instances']} | {report['invocations']} "
            f"| {report['nodes_per_invocation']} | {EVENT_LOOP_BACKPRESSURE} "
            f"| {median(r['total_seconds'] for r in rounds):.3f} "
            f"| {median(r['build_seconds'] for r in rounds):.3f} "
            f"| {median(r['nodes_per_second'] for r in rounds):.1f} |"
        )
    return "\n".join(lines) + "\n"


def pytest_sessionfinish(session, exitstatus):
    """Persist reports after the suite, independently of terminal output."""
    config = session.config
    reports = getattr(config, "_graph_metrics", [])
    if not reports:
        return
    readme = config.rootpath / "test-results" / "README.md"
    readme.parent.mkdir(parents=True, exist_ok=True)
    readme.write_text(_performance_table(reports), encoding="utf-8")
    destination = config.getoption("--graph-report")
    if destination:
        path = Path(destination).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "event_loop_backpressure": EVENT_LOOP_BACKPRESSURE,
            "event_pipe_max_len": EVENT_PIPE_MAX_LEN,
            "logging": "ERROR; event dispatch logging disabled",
            "node_workload": "Immediate synchronous Command return with one visit marker; no sleeps, waits, I/O, or runtime probes.",
            "timing": "Fresh graph construction through completion of every invocation; validation and decomposition excluded.",
            "latency": "Completion time measured from the start of graph construction, including queueing.",
            "results": reports,
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def pytest_terminal_summary(terminalreporter, config):
    reports = getattr(config, "_graph_metrics", [])
    if not reports:
        return
    terminalreporter.section("Graph performance (construction included)")
    for report in reports:
        rounds = report["rounds"]
        terminalreporter.write_line(
            f"{report['ownership']}/{report['topology']}: "
            f"{report['graph_instances']} graphs, {report['invocations']} invocations, "
            f"{report['nodes_per_invocation']} nodes/invocation; "
            f"median total={median(r['total_seconds'] for r in rounds):.3f}s, "
            f"build={median(r['build_seconds'] for r in rounds):.3f}s, "
            f"{median(r['invocations_per_second'] for r in rounds):.1f} invocations/s, "
            f"{median(r['nodes_per_second'] for r in rounds):.1f} nodes/s, "
            f"p95={median(r['latency_p95_ms'] for r in rounds):.1f}ms"
        )
    terminalreporter.write_line(f"Graph results: {config.rootpath / 'test-results' / 'README.md'}")
    destination = config.getoption("--graph-report")
    if destination:
        terminalreporter.write_line(f"Graph metrics: {Path(destination).resolve()}")

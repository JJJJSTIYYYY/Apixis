"""Repository-wide pytest selection and graph benchmark options."""

import math

import pytest


def pytest_addoption(parser):
    group = parser.getgroup("graph performance")
    group.addoption(
        "--run-performance", action="store_true", default=False,
        help="Collect the opt-in graph performance suite.",
    )
    for option, default, help_text in (
        ("--graph-count", 1000, "Concurrent invocations; independent mode builds this many graphs."),
        ("--graph-nodes", 16, "Minimal graph nodes executed by each invocation."),
        ("--graph-width", 4, "Maximum nodes per concurrent graph step."),
        ("--graph-repeats", 3, "Measured rounds; every round includes fresh graph construction."),
    ):
        group.addoption(option, type=_positive_int, default=default, help=help_text)
    group.addoption(
        "--graph-timeout", type=_positive_float, default=120.0,
        help="Deadline in seconds for each entire build-and-run round.",
    )
    group.addoption("--graph-report", default=None, help="Write benchmark metrics to a JSON file.")


def _positive_int(value):
    number = int(value)
    if number <= 0:
        raise ValueError("must be a positive integer")
    return number


def _positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("must be a finite positive number")
    return number


def pytest_collection_modifyitems(config, items):
    """Classify existing tests without repeating markers in every module."""
    for item in items:
        parts = item.path.parts
        if "integration" in parts:
            item.add_marker(pytest.mark.integration)
        elif "core" in parts:
            item.add_marker(pytest.mark.unit)
        if "performance" in parts and not config.getoption("--run-performance"):
            item.add_marker(pytest.mark.skip(reason="use --run-performance to run benchmarks"))


def pytest_ignore_collect(collection_path, config):
    if collection_path.name == "performance":
        return not config.getoption("--run-performance")



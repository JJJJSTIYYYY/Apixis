"""Bounded pending logs and FIFO-preserving disk flushes."""

import asyncio
from collections import deque
import importlib

import pytest

from apixis import Logger


@pytest.fixture
def log_output(monkeypatch, tmp_path):
    """Use a small shared buffer and real files without starting a flush task."""
    module = importlib.import_module("apixis.core.utils.logger")
    monkeypatch.setattr(module, "LOG_BUFFER_SIZE", 3)
    monkeypatch.setattr(module, "BASE_DIR", str(tmp_path))
    monkeypatch.setattr(module, "DEBUG_LEVEL", "DEBUG")
    monkeypatch.setattr(Logger, "log_cache", deque(maxlen=3))
    monkeypatch.setattr(Logger, "log_cache_size", 0)
    monkeypatch.setattr(Logger, "flush_event", asyncio.Event())
    monkeypatch.setattr(Logger, "current_log_file_index", {})
    monkeypatch.setattr(Logger, "current_log_date", {})
    return tmp_path


async def test_fifo_is_shared_across_logger_names_and_flush_preserves_order(log_output):
    first, second = Logger("first"), Logger("second")
    first.info("discard-this-record")
    second.info("keep-second-1")
    first.info("keep-first-1")
    second.info("keep-second-2")
    await Logger.flush()

    first_text = next((log_output / "first").glob("*.log")).read_text()
    second_text = next((log_output / "second").glob("*.log")).read_text()
    assert "discard-this-record" not in first_text
    assert "keep-first-1" in first_text
    assert "keep-second-1" in second_text
    assert second_text.index("keep-second-1") < second_text.index("keep-second-2")
    assert len(first_text.splitlines()) + len(second_text.splitlines()) == 3


async def test_fifo_stays_bounded_after_flush_and_does_not_repeat_records(log_output):
    logger = Logger("repeat")
    logger.info("first-batch")
    await Logger.flush()
    for index in range(5):
        logger.info(f"next-batch-{index}")
    await Logger.flush()
    await Logger.flush()

    text = next((log_output / "repeat").glob("*.log")).read_text()
    assert text.count("first-batch") == 1
    assert "next-batch-0" not in text
    assert "next-batch-1" not in text
    assert [line.rsplit(": ", 1)[-1] for line in text.splitlines()] == [
        "first-batch", "next-batch-2", "next-batch-3", "next-batch-4",
    ]


def test_evicted_records_do_not_keep_triggering_size_based_flush(log_output, monkeypatch):
    monkeypatch.setattr(Logger, "max_cache_size", 512)
    logger = Logger("bounded")
    logger.info("x" * 1024)
    assert Logger.flush_event.is_set()
    for _ in range(3):
        logger.info("short")
    Logger.flush_event.clear()
    logger.info("latest")
    assert not Logger.flush_event.is_set()

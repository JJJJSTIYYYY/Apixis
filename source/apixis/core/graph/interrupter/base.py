from asyncio import Future
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Block:
    run_id: str
    block_id: str
    namespace: str
    with_data: Any

    _future: Future[Any] = field(repr=False)
    graph_id: str | None = None

    def __post_init__(self) -> None:
        """Reject manually constructed blocks without an awaitable future."""
        if not isinstance(self._future, Future):
            raise TypeError("Block._future must be an asyncio.Future.")

    def __await__(self):
        return self._future.__await__()

    @property
    def done(self) -> bool:
        """Return whether this interruption has already been completed."""
        return self._future.done()

    @property
    def cancelled(self) -> bool:
        """Return whether this interruption was cancelled externally."""
        return self._future.cancelled()

    def resolve(self, result: Any) -> None:
        """Resolve the block with `result`, unblocking the interrupted execution."""
        if self._future.done():
            return

        self._future.set_result(result)

    def fail(self, error: Exception) -> None:
        """Unblock the waiting node by raising an interruption failure."""
        if not self._future.done():
            self._future.set_exception(error)

    def cancel(self) -> None:
        """Cancel the block and abort its current graph invocation."""
        self._future.cancel()

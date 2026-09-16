"""Public exceptions, logging, and lifecycle utilities."""

from apixis.core.utils.exception import (
    BlockHookNotRegisteredError,
    BlockNotResolvedError,
    EventChannelError,
    EventChannelPermissionError,
    EventChannelUnavailableError,
    EventHandlerAlreadyRegisteredError,
    EventHandlerNotRegisteredError,
    GraphNodeError,
    InvalidContextError,
    InvalidNodeReturnsError,
)
from apixis.core.utils.logger import (
    Logger,
    logger,
)

__all__ = [
    "BlockHookNotRegisteredError",
    "BlockNotResolvedError",
    "EventChannelError",
    "EventChannelPermissionError",
    "EventChannelUnavailableError",
    "EventHandlerAlreadyRegisteredError",
    "EventHandlerNotRegisteredError",
    "GraphNodeError",
    "InvalidContextError",
    "InvalidNodeReturnsError",
    "Logger",
    "logger",
]

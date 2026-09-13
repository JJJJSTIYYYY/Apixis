"""Public exceptions, logging, and lifecycle utilities."""

from apixis.core.utils.exception import (
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
from apixis.core.utils.lifespan import (
    auto_init,
    resource_cleaner,
)

__all__ = [
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
    "auto_init",
    "resource_cleaner",
]

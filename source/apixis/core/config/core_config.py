"""Core runtime settings backed by the shared configuration loader."""

from typing import Literal
from uuid import uuid4

from apixis.core.config.base import VERSION, _get_config


# Remote gateway and node identity
REMOTE_GATEWAY_ENABLE = _get_config("REMOTE_GATEWAY.enable", False) is True
REMOTE_GATEWAY_BASE_URL = _get_config(
    "REMOTE_GATEWAY.base_url", "http://localhost:8080"
)
REMOTE_GATEWAY_CONFIG_ENDPOINT = _get_config(
    "REMOTE_GATEWAY.config_endpoint", "/api/config"
)
REMOTE_GATEWAY_PIPE_ENDPOINT = _get_config(
    "REMOTE_GATEWAY.pipe_endpoint", "/api/pipe"
)
GATEWAY_MAX_RETRY = _get_config("REMOTE_GATEWAY.max_retry", 5)
GATEWAY_RETRY_INITIAL_DELAY = _get_config(
    "REMOTE_GATEWAY.retry_initial_delay", 1.0
)
GATEWAY_TIMEOUT = _get_config("REMOTE_GATEWAY.timeout", 10.0)


def _create_node_id(remote_enabled: bool) -> str:
    """Create a globally unique MQ id only for remote node mode."""
    return uuid4().hex if remote_enabled else "apix_service"


NODE_ID = _create_node_id(REMOTE_GATEWAY_ENABLE)


# Local runtime paths and node identity
BASE_DIR = _get_config("SERVER.base_dir", "./.apix_data/")
NODE_NAME = _get_config("SERVER.node_name", "apix_service")


# Log
DEBUG_LEVEL: Literal["DEBUG", "INFO", "WARN", "ERROR"] = _get_config(
    "LOG.debug_level",
    "DEBUG",
).upper()

TRACE = _get_config("LOG.trace", True)
SHOW_EVENT_DISPATCH = _get_config("LOG.show_event_dispatch", True)
MAX_LOG_FILE_SIZE = _get_config("LOG.max_log_file_size", 10 * 1024 * 1024)


# Pipeline
EVENT_PIPE_MAX_LEN = _get_config("PIPELINE.event_pipe_max_len", 65536)
EVENT_LOOP_BACKPRESSURE = _get_config("PIPELINE.event_loop_backpressure", 1024)
BACKGROUND_HANDLER_BACKPRESSURE = _get_config("PIPELINE.background_handler_backpressure", 4096)


# External event mailbox
EVENT_CHANNEL_CONFIG = _get_config(
    "EVENT_CHANNEL", {}
)
EVENT_CHANNEL_TYPE: Literal["kafka", "rabbitmq"] = _get_config(
    "EVENT_CHANNEL.type", "kafka"
)
KAFKA_BOOTSTRAP_SERVERS = _get_config(
    "EVENT_CHANNEL.kafka.bootstrap_servers", ["localhost:9092"]
)
KAFKA_TOPIC_PREFIX = _get_config(
    "EVENT_CHANNEL.kafka.topic_prefix", "apixis.mailbox"
)
KAFKA_GROUP_ID_PREFIX = _get_config(
    "EVENT_CHANNEL.kafka.group_id_prefix", "apixis.node"
)
RABBITMQ_URL = _get_config(
    "EVENT_CHANNEL.rabbitmq.url", "amqp://guest:guest@localhost/"
)
RABBITMQ_EXCHANGE = _get_config(
    "EVENT_CHANNEL.rabbitmq.exchange", "apixis.events"
)
RABBITMQ_QUEUE_PREFIX = _get_config(
    "EVENT_CHANNEL.rabbitmq.queue_prefix", "apixis.mailbox"
)
RABBITMQ_PREFETCH_COUNT = _get_config(
    "EVENT_CHANNEL.rabbitmq.prefetch_count", 100
)


# Resource cleanup remains part of Core, independently of Agent and storage.
# Accept the original key so existing cleanup intervals remain effective.
CACHE_CLEAN_INTERVAL = _get_config(
    "RUNTIME.cache_clean_interval",
    _get_config("AGENT_RUNTIME.cache_clean_interval", 300),
)

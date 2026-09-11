import os
from collections.abc import Mapping
from typing import Any

import httpx
import yaml


# Global configuration settings for Apixis Core.
VERSION = "0.0.1"

# These sections describe resources owned by one concrete node.  They must
# never be inherited from the gateway, otherwise several nodes may consume the
# same mailbox configuration and lose destination isolation.
_NODE_LOCAL_CONFIG_SECTIONS = frozenset({"EVENT_CHANNEL"})

def _load_from_yaml(path: str) -> dict[str, Any]:
    """Load configuration from a local YAML file."""
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if data is None:
        return {}

    if not isinstance(data, dict):
        raise ValueError(
            f"Config file must contain a YAML mapping, got {type(data).__name__}."
        )

    return data


def _load_from_remote(
    base_url: str,
    endpoint: str,
) -> dict[str, Any]:
    """Load configuration from the remote config center."""
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    response = httpx.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(
            "Remote config center must return a JSON object, "
            f"got {type(data).__name__}."
        )

    return data


def _merge_config(
    remote: Mapping[str, Any],
    local: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Recursively merge configuration mappings.

    Local values always take precedence over remote values. Nested mappings
    are merged recursively, so a local partial section does not discard other
    remote values in that section.
    """
    merged = dict(remote)

    for key, local_value in local.items():
        remote_value = merged.get(key)

        if (
            isinstance(remote_value, Mapping)
            and isinstance(local_value, Mapping)
        ):
            merged[key] = _merge_config(remote_value, local_value)
        else:
            merged[key] = local_value

    return merged


def _filter_remote_config(remote: Mapping[str, Any]) -> dict[str, Any]:
    """Remove node-local sections from gateway-provided configuration.

    The returned mapping is a new shallow copy; the gateway response itself is
    left untouched.  A local section is subsequently merged as usual, but no
    missing nested value can be backfilled by the remote configuration.
    """
    return {
        key: value
        for key, value in remote.items()
        if key not in _NODE_LOCAL_CONFIG_SECTIONS
    }


def _load_config(path: str) -> dict[str, Any]:
    """
    Load the effective configuration.

    Loading order:
        1. Read local YAML.
        2. Discover REMOTE_GATEWAY from the local YAML.
        3. Load remote configuration when configured.
        4. Remove node-local sections such as EVENT_CHANNEL from remote data.
        5. Merge local configuration over the filtered remote configuration.
    """
    local_config = _load_from_yaml(path)

    remote_center = local_config.get("REMOTE_GATEWAY")
    if remote_center is None:
        return local_config

    if not isinstance(remote_center, Mapping):
        raise ValueError("REMOTE_GATEWAY must be a mapping.")

    enable = remote_center.get("enable", False)
    if not isinstance(enable, bool):
        raise ValueError("REMOTE_GATEWAY.enable must be a boolean.")
    if enable is not True:
        return local_config

    center_base_url = remote_center.get("base_url")
    config_endpoint = remote_center.get("config_endpoint")

    if not isinstance(center_base_url, str) or not center_base_url.strip():
        raise ValueError(
            "REMOTE_GATEWAY.base_url must be a non-empty string."
        )

    if not isinstance(config_endpoint, str) or not config_endpoint.strip():
        raise ValueError(
            "REMOTE_GATEWAY.config_endpoint must be a non-empty string."
        )

    remote_config = _load_from_remote(
        base_url=center_base_url,
        endpoint=config_endpoint,
    )

    return _merge_config(
        remote=_filter_remote_config(remote_config),
        local=local_config,
    )


def _get_config(path: str, default=None):
    """Read a dotted path, falling back for missing, non-mapping or null values."""
    value = _config

    for key in path.split("."):
        if not isinstance(value, Mapping):
            return default

        if key not in value:
            return default

        value = value[key]

    return default if value is None else value


# Load once in the module that owns configuration lookup.
_config = _load_config("./config.yaml")

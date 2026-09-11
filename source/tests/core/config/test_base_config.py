"""Tests for shared configuration loading and core runtime settings."""

import pytest

from apixis.core.config import base, core_config


def test_remote_config_requires_explicit_true(monkeypatch):
    local = {"REMOTE_GATEWAY": {"enable": False}}
    monkeypatch.setattr(
        base, "_load_from_remote", lambda **kwargs: pytest.fail("remote load")
    )
    monkeypatch.setattr(base, "_load_from_yaml", lambda path: local)

    assert base._load_config("config.yaml") is local


def test_enabled_remote_config_is_loaded_and_local_wins(monkeypatch):
    local = {
        "REMOTE_GATEWAY": {
            "enable": True,
            "base_url": "http://gateway",
            "config_endpoint": "/config",
        },
        "SERVER": {"node_name": "local-node"},
    }
    calls = []
    monkeypatch.setattr(base, "_load_from_yaml", lambda path: local)
    monkeypatch.setattr(
        base,
        "_load_from_remote",
        lambda **kwargs: calls.append(kwargs)
        or {"SERVER": {"node_name": "remote-node", "base_dir": "/remote"}},
    )

    result = base._load_config("config.yaml")

    assert calls == [{"base_url": "http://gateway", "endpoint": "/config"}]
    assert result["SERVER"] == {"node_name": "local-node", "base_dir": "/remote"}


def test_remote_event_channel_is_ignored_when_local_section_is_missing(
    monkeypatch,
):
    local = {
        "REMOTE_GATEWAY": {
            "enable": True,
            "base_url": "http://gateway",
            "config_endpoint": "/config",
        }
    }
    remote = {
        "EVENT_CHANNEL": {
            "type": "rabbitmq",
            "rabbitmq": {"url": "amqp://shared-gateway-mailbox/"},
        },
        "SERVER": {"node_name": "remote-node"},
    }
    monkeypatch.setattr(base, "_load_from_yaml", lambda path: local)
    monkeypatch.setattr(
        base,
        "_load_from_remote",
        lambda **kwargs: remote,
    )

    result = base._load_config("config.yaml")

    assert "EVENT_CHANNEL" not in result
    assert result["SERVER"] == {"node_name": "remote-node"}
    assert remote["EVENT_CHANNEL"]["type"] == "rabbitmq"


def test_remote_event_channel_cannot_fill_partial_local_section(monkeypatch):
    local_event_channel = {
        "type": "kafka",
        "kafka": {"bootstrap_servers": ["node-a-broker:9092"]},
    }
    local = {
        "REMOTE_GATEWAY": {
            "enable": True,
            "base_url": "http://gateway",
            "config_endpoint": "/config",
        },
        "EVENT_CHANNEL": local_event_channel,
    }
    monkeypatch.setattr(base, "_load_from_yaml", lambda path: local)
    monkeypatch.setattr(
        base,
        "_load_from_remote",
        lambda **kwargs: {
            "EVENT_CHANNEL": {
                "type": "rabbitmq",
                "kafka": {"topic_prefix": "shared-mailbox"},
                "rabbitmq": {"url": "amqp://shared/"},
            }
        },
    )

    result = base._load_config("config.yaml")

    assert result["EVENT_CHANNEL"] == local_event_channel
    assert "topic_prefix" not in result["EVENT_CHANNEL"]["kafka"]
    assert "rabbitmq" not in result["EVENT_CHANNEL"]


@pytest.mark.parametrize("enable", [1, "true", None])
def test_remote_enable_must_be_boolean(monkeypatch, enable):
    monkeypatch.setattr(
        base,
        "_load_from_yaml",
        lambda path: {"REMOTE_GATEWAY": {"enable": enable}},
    )
    with pytest.raises(ValueError, match="must be a boolean"):
        base._load_config("config.yaml")


def test_remote_node_id_is_uuid4_hex():
    node_id = core_config._create_node_id(True)
    assert len(node_id) == 32
    assert int(node_id, 16) >= 0
    assert core_config._create_node_id(False) == "apix_service"


def test_external_channel_defaults_are_available():
    assert core_config.EVENT_CHANNEL_TYPE in {"kafka", "rabbitmq"}
    assert core_config.KAFKA_BOOTSTRAP_SERVERS
    assert core_config.RABBITMQ_URL.startswith("amqp")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("nested.value", 42),
        ("nested.zero", 0),
        ("nested.disabled", False),
        ("nested.empty", ""),
        ("nested.null", "fallback"),
        ("nested.absent", "fallback"),
        ("nested.value.child", "fallback"),
        ("missing", "fallback"),
    ],
)
def test_get_config_uses_shared_mapping_and_preserves_falsey_values(
    monkeypatch, path, expected
):
    """Lookup works in base.py without an undefined or duplicated _config."""
    monkeypatch.setattr(base, "_config", {
        "nested": {"value": 42, "zero": 0, "disabled": False, "empty": "", "null": None}
    })

    assert base._get_config(path, "fallback") == expected
    assert core_config._get_config(path, "fallback") == expected


def test_yaml_missing_or_empty_file_uses_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    assert base._load_from_yaml(str(path)) == {}
    path.write_text("", encoding="utf-8")
    assert base._load_from_yaml(str(path)) == {}


def test_yaml_rejects_non_mapping(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("- item\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain a YAML mapping"):
        base._load_from_yaml(str(path))


@pytest.mark.parametrize("remote", [[], {"enable": True}, {
    "enable": True, "base_url": "http://gateway", "config_endpoint": " "
}])
def test_remote_configuration_validation_is_preserved(monkeypatch, remote):
    monkeypatch.setattr(base, "_load_from_yaml", lambda path: {"REMOTE_GATEWAY": remote})
    with pytest.raises(ValueError, match="REMOTE_GATEWAY"):
        base._load_config("config.yaml")

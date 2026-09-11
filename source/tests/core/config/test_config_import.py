"""Verify configuration initialization through real, isolated package imports."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


@pytest.mark.parametrize(
    ("settings", "expected_interval"),
    [
        ({}, 300),
        ({"RUNTIME": {"cache_clean_interval": 17}}, 17),
        ({"RUNTIME": {"cache_clean_interval": 0}}, 0),
        ({"AGENT_RUNTIME": {"cache_clean_interval": 23}}, 23),
        ({"RUNTIME": {"cache_clean_interval": 17},
          "AGENT_RUNTIME": {"cache_clean_interval": 23}}, 17),
        ({"REMOTE_GATEWAY": {"enable": True, "base_url": "http://gateway",
                             "config_endpoint": "/config"}}, 31),
    ],
)
def test_package_import_loads_config_once_without_agent_or_storage(
    tmp_path, settings, expected_interval
):
    """A remote core can import without Agent, database or cache configuration."""
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")
    script = '''
import json
import httpx
import yaml

loads = []
requests = []
original_load = yaml.safe_load

def counted_load(stream):
    loads.append(True)
    return original_load(stream)

def remote_get(url, **kwargs):
    requests.append((url, kwargs))
    return httpx.Response(
        200,
        json={"RUNTIME": {"cache_clean_interval": 31},
              "EVENT_CHANNEL": {"type": "rabbitmq"}},
        request=httpx.Request("GET", url),
    )

yaml.safe_load = counted_load
httpx.get = remote_get

from apixis.core.config import base, core_config
from apixis.core.utils.lifespan import resource_cleaner

assert len(loads) == 1
assert core_config._get_config is base._get_config
assert resource_cleaner._interval == core_config.CACHE_CLEAN_INTERVAL
assert core_config.EVENT_CHANNEL_TYPE == "kafka"
assert not any(name in vars(core_config) for name in (
    "DATA_STORE_TYPE", "CACHE_STORE_TYPE", "PROVIDER_BASE_URL",
    "TOOLS_MAX_OUTPUT_LENGTH", "ORIGINAL_PROXY_ENV",
))
if core_config.REMOTE_GATEWAY_ENABLE:
    assert requests == [("http://gateway/config", {"timeout": 10})]
else:
    assert requests == []
print(json.dumps({"interval": core_config.CACHE_CLEAN_INTERVAL}))
'''
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3]))
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    # The existing logger may emit startup messages before the JSON result.
    assert json.loads(result.stdout.splitlines()[-1])["interval"] == expected_interval

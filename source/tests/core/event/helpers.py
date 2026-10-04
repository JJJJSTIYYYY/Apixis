"""Shared transport doubles for event channel contract tests."""

import time

import httpx

from apixis.core.event import ApixEvent, EventType, GatewayChannel


def make_event(name: str = "test.event") -> ApixEvent:
    return ApixEvent(
        event_id="event-1",
        event_type=EventType.INFO,
        event_name=name,
        context={"value": 1},
        timestamp=time.time(),
        accepted=False,
    )


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False

    async def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def aclose(self):
        self.closed = True


def response(status: int = 200, payload=None) -> httpx.Response:
    return httpx.Response(
        status,
        request=httpx.Request("GET", "http://gateway/api/pipe"),
        json={} if payload is None else payload,
    )


def make_gateway(client) -> GatewayChannel:
    return GatewayChannel(
        base_url="http://gateway",
        pipe_endpoint="/api/pipe",
        node_id="node-a",
        node_name="Alice",
        channel_type="kafka",
        max_retry=2,
        retry_initial_delay=0.1,
        timeout=1,
        client=client,
    )

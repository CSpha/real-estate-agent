import requests

from app.ops import heartbeat
from app.ops.heartbeat import ping_heartbeat


URL = "https://hc-ping.com/00000000-0000-0000-0000-secret-check"


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_ping_sends_a_get_with_a_timeout():
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(200)

    assert ping_heartbeat(url=URL, get=fake_get)
    assert calls == [(URL, {"timeout": 10})]


def test_ping_skips_when_no_url_is_configured(monkeypatch):
    monkeypatch.setattr(heartbeat, "heartbeat_url", lambda: None)
    calls = []

    assert not ping_heartbeat(get=lambda *args, **kwargs: calls.append(1))
    assert calls == []


def test_ping_failure_never_raises_or_prints_the_url(capsys):
    def failing_get(url, **kwargs):
        return _Response(404)

    def offline_get(url, **kwargs):
        raise requests.ConnectionError(f"cannot reach {url}")

    assert not ping_heartbeat(url=URL, get=failing_get)
    assert not ping_heartbeat(url=URL, get=offline_get)

    output = capsys.readouterr().out
    assert "HTTP 404" in output
    assert "ConnectionError" in output
    assert "secret-check" not in output

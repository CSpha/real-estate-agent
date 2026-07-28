import requests

from app.alerts.outbox import is_retryable_slack_error


def http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(response=response)


def test_transient_slack_errors_are_retryable():
    assert is_retryable_slack_error(requests.Timeout()) is True
    assert is_retryable_slack_error(http_error(429)) is True
    assert is_retryable_slack_error(http_error(503)) is True


def test_bad_slack_request_is_permanent():
    assert is_retryable_slack_error(http_error(400)) is False

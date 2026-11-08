from unittest.mock import MagicMock

import pytest

from app.alerts import outbox
from app.alerts import process_alert_outbox as worker_module
from app.alerts.formatting import (
    build_digest_message,
    escape,
    header_block,
    money,
    percent,
    zillow_url,
)
from app.alerts.process_alert_outbox import SUMMARIZERS
from app.alerts.send_potential_deal_alerts import (
    format_slack_message as format_deal,
    summarize_event as summarize_deal,
)
from app.alerts.send_price_drop_alerts import (
    format_slack_message as format_drop,
    summarize_event as summarize_drop,
)
from app.alerts.slack import SlackDeliveryError, validate_payload


DROP = {
    "address": "3763 Bates Rd",
    "city": "Wooster",
    "state": "OH",
    "previous_price": "324900.00",
    "current_price": "295000.00",
    "price_change": "-29900.00",
    "price_change_pct": "-9.20",
    "source_listing_id": "MLSNOW:5236901",
}
DEAL = {
    "address": "211 S Elm St",
    "city": "Orrville",
    "state": "OH",
    "county_name": "Wayne",
    "list_price": "139900",
    "county_median_sale_price": "249342",
    "price_vs_county_median": "-109442",
    "pct_below_or_above_median": "-0.4389",
    "market_score": "90",
    "score_reason": "Price is at least 30% below county median",
    "source_listing_id": "MLSNOW:5243252",
}


def test_money_and_percent_formatting():
    assert money("324900.00") == "$324,900"
    assert money("-29900.00") == "-$29,900"
    assert money(None) == "N/A"
    assert money("None") == "N/A"
    assert percent("-9.2") == "-9.20%"
    assert percent("-0.4389", fraction=True) == "-43.89%"


def test_escape_and_header_length():
    assert escape("A & B <x>") == "A &amp; B &lt;x&gt;"
    assert len(header_block("x" * 200)["text"]["text"]) == 150


def test_price_drop_message_is_readable_and_valid():
    message = format_drop(DROP)
    validate_payload(message)
    assert "Previous price: $324,900" in message["text"]
    assert "Change: -$29,900 (-9.20%)" in message["text"]
    fields = [field["text"] for field in message["blocks"][2]["fields"]]
    assert fields == ["*Now*\n$295,000", "*Was*\n$324,900", "*Change*\n-$29,900 (-9.20%)"]
    assert message["blocks"][1]["text"]["text"] == (
        "*<https://www.zillow.com/homes/3763-Bates-Rd,-Wooster,-OH_rb/"
        "|3763 Bates Rd, Wooster, OH>*"
    )


def test_zillow_url_encodes_unsafe_address_characters():
    event = {"address": "12 Main St #4/B", "city": "Orrville", "state": "OH"}
    assert zillow_url(event) == (
        "https://www.zillow.com/homes/12-Main-St-%234%2FB,-Orrville,-OH_rb/"
    )


def test_potential_deal_message_is_readable_and_valid():
    message = format_deal(DEAL)
    validate_payload(message)
    assert message["text"].startswith("Potential deal: 211 S Elm St, Orrville, OH")
    assert "Difference from median: -$109,442" in message["text"]
    assert message["blocks"][0]["type"] == "header"


def test_summaries_describe_direction():
    rich, plain = summarize_drop(DROP)
    assert plain == (
        "Price drop: 3763 Bates Rd, Wooster, OH, $295,000, down $29,900 "
        "(-9.20%) from $324,900"
    )
    assert rich.startswith(":chart_with_downwards_trend: *<https://")
    _rich, plain = summarize_deal(DEAL)
    assert "43.89% below the county median, score 90" in plain


def test_digest_message_lists_each_alert():
    message = build_digest_message(
        [("price_drop", DROP), ("potential_deal", DEAL)], SUMMARIZERS
    )
    validate_payload(message)
    assert message["text"].splitlines()[0] == "2 listing alerts"
    assert len(message["blocks"]) == 4


def test_validate_payload_rejects_bad_blocks():
    with pytest.raises(SlackDeliveryError):
        validate_payload({"text": "x", "blocks": []})
    with pytest.raises(SlackDeliveryError):
        validate_payload({"text": "x", "blocks": [{}] * 51})


@pytest.fixture
def fake_digest_outbox(monkeypatch):
    queue = []
    state = {"sent": [], "failed": [], "released": []}

    def claim(_engine, batch_size):
        batch = queue[:batch_size]
        del queue[:batch_size]
        return batch

    monkeypatch.setattr(outbox, "count_due_alerts", lambda _engine: len(queue))
    monkeypatch.setattr(outbox, "claim_alerts", claim)
    monkeypatch.setattr(
        outbox, "mark_alert_sent", lambda alert, _engine: state["sent"].append(alert["id"])
    )
    monkeypatch.setattr(
        outbox,
        "mark_alert_failed",
        lambda alert, error, **kwargs: state["failed"].append((alert["id"], kwargs)),
    )
    monkeypatch.setattr(
        outbox,
        "release_alerts",
        lambda alerts, _engine: state["released"].extend(a["id"] for a in alerts),
    )

    def add(count, payload=DROP):
        start = len(queue) + 1
        queue.extend(
            {"id": i, "alert_type": "price_drop", "attempt_count": 1, "payload_json": payload}
            for i in range(start, start + count)
        )

    state["add"] = add
    return state


def _deliver(send, *, limit=100):
    return outbox.deliver_pending_alerts(
        "https://example.invalid/hook",
        {"price_drop": format_drop},
        send=send,
        engine=MagicMock(),
        limit=limit,
        sleep=MagicMock(),
        monotonic=lambda: 0.0,
        summarizers=SUMMARIZERS,
    )


def test_few_alerts_are_sent_individually(fake_digest_outbox, monkeypatch):
    fake_digest_outbox["add"](3)
    queue_claims = []
    monkeypatch.setattr(
        outbox,
        "claim_next_alert",
        lambda engine: queue_claims.append(1) or (outbox.claim_alerts(engine, 1) or [None])[0],
    )
    send = MagicMock()

    assert _deliver(send) == {"sent": 3, "failed": 0}
    assert send.call_count == 3
    assert len(queue_claims) == 4


def _digest_headers(send):
    return [call.args[1]["text"].splitlines()[0] for call in send.call_args_list]


def test_many_alerts_are_sent_as_balanced_digests(fake_digest_outbox):
    fake_digest_outbox["add"](12)
    send = MagicMock()

    assert _deliver(send) == {"sent": 12, "failed": 0}
    assert _digest_headers(send) == ["6 listing alerts", "6 listing alerts"]
    assert fake_digest_outbox["sent"] == list(range(1, 13))


def test_eleven_due_alerts_make_digests_of_six_and_five(fake_digest_outbox):
    fake_digest_outbox["add"](11)
    send = MagicMock()

    assert _deliver(send) == {"sent": 11, "failed": 0}
    assert _digest_headers(send) == ["6 listing alerts", "5 listing alerts"]
    assert fake_digest_outbox["sent"] == list(range(1, 12))


@pytest.mark.parametrize(
    "due,expected",
    [(4, [4]), (10, [10]), (21, [7, 7, 7])],
)
def test_digest_counts_match_the_due_count(fake_digest_outbox, due, expected):
    fake_digest_outbox["add"](due)
    send = MagicMock()

    assert _deliver(send) == {"sent": due, "failed": 0}
    assert _digest_headers(send) == [f"{size} listing alerts" for size in expected]


@pytest.mark.parametrize(
    "due,limit,expected",
    [
        (0, None, []),
        (-1, None, []),
        (4, None, [4]),
        (10, None, [10]),
        (11, None, [6, 5]),
        (20, None, [10, 10]),
        (21, None, [7, 7, 7]),
        (25, None, [9, 8, 8]),
        (25, 12, [6, 6]),
        (25, 10, [10]),
        (25, 4, [4]),
        (25, 0, []),
        (5, 100, [5]),
    ],
)
def test_digest_sizes(due, limit, expected):
    assert outbox.digest_sizes(due, limit) == expected


@pytest.mark.parametrize("due", range(1, 120))
def test_digest_sizes_are_even_bounded_and_never_exceed_the_old_message_count(due):
    sizes = outbox.digest_sizes(due)
    old_message_count = -(-due // outbox.DIGEST_SIZE)
    assert sum(sizes) == due
    assert len(sizes) == old_message_count
    assert max(sizes) <= outbox.DIGEST_SIZE
    assert max(sizes) - min(sizes) <= 1
    assert sizes == sorted(sizes, reverse=True)


def test_digest_respects_the_limit(fake_digest_outbox):
    fake_digest_outbox["add"](12)
    send = MagicMock()

    assert _deliver(send, limit=5) == {"sent": 5, "failed": 0}
    assert send.call_args.args[1]["text"].startswith("5 listing alerts")


def test_limit_caps_the_digest_count_too(fake_digest_outbox):
    fake_digest_outbox["add"](25)
    send = MagicMock()

    assert _deliver(send, limit=12) == {"sent": 12, "failed": 0}
    assert _digest_headers(send) == ["6 listing alerts", "6 listing alerts"]


def test_unusable_webhook_fails_one_digest_alert_and_releases_the_rest(fake_digest_outbox):
    fake_digest_outbox["add"](12)
    send = MagicMock(
        side_effect=SlackDeliveryError("404", retryable=False, webhook_unusable=True)
    )

    assert _deliver(send) == {"sent": 0, "failed": 1}
    assert [alert_id for alert_id, _ in fake_digest_outbox["failed"]] == [1]
    assert fake_digest_outbox["released"] == list(range(2, 7))
    assert send.call_count == 1


def test_rate_limited_digest_retries_every_alert_and_stops(fake_digest_outbox):
    fake_digest_outbox["add"](12)
    send = MagicMock(
        side_effect=SlackDeliveryError(
            "429", retryable=True, rate_limited=True, retry_after_seconds=30
        )
    )

    assert _deliver(send) == {"sent": 0, "failed": 6}
    assert all(kwargs["rate_limited"] for _id, kwargs in fake_digest_outbox["failed"])
    assert send.call_count == 1


def test_bad_payload_is_dropped_from_the_digest(fake_digest_outbox):
    fake_digest_outbox["add"](4)
    fake_digest_outbox["add"](1, payload={"address": "missing fields"})
    send = MagicMock()

    assert _deliver(send) == {"sent": 4, "failed": 1}
    assert fake_digest_outbox["failed"][0][0] == 5
    assert fake_digest_outbox["failed"][0][1]["retryable"] is False


def test_preview_uses_the_same_balanced_digests_as_delivery(monkeypatch):
    previews = [
        (
            {
                "id": i,
                "alert_type": "price_drop",
                "source_listing_id": f"l{i}",
                "payload_json": DROP,
            },
            {"text": "one"},
            None,
        )
        for i in range(1, 12)
    ]
    monkeypatch.setattr(worker_module, "preview_due_alerts", lambda *a, **k: previews)
    monkeypatch.setattr(
        worker_module,
        "get_settings",
        lambda: type("S", (), {"slack_test_webhook_url": "https://example.invalid/test"})(),
    )
    send = MagicMock()

    counts = worker_module.preview_alert_outbox(
        test_channel=True, send=send, sleep=MagicMock()
    )

    assert counts["test_sent"] == 2
    assert [call.args[1]["text"].splitlines()[0] for call in send.call_args_list] == [
        "6 listing alerts",
        "5 listing alerts",
    ]

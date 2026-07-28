import app.run_pipeline as pipeline_module


def test_skip_alerts_never_calls_slack_sender(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline_module,
        "load_sample_csv",
        lambda _path: calls.append("load"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "snapshot_current_listings",
        lambda: calls.append("snapshot"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "score_listings_against_market",
        lambda: calls.append("score"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "evaluate_enabled_searches",
        lambda: calls.append("evaluate") or [],
    )
    monkeypatch.setattr(
        pipeline_module,
        "queue_price_drop_alerts",
        lambda: calls.append("queue") or 0,
    )

    pipeline_module.run_pipeline(queue_alerts=False)

    assert calls == ["load", "snapshot", "score", "evaluate"]


def test_saved_search_evaluation_can_be_skipped(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline_module,
        "load_sample_csv",
        lambda _path: calls.append("load"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "snapshot_current_listings",
        lambda: calls.append("snapshot"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "score_listings_against_market",
        lambda: calls.append("score"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "evaluate_enabled_searches",
        lambda: calls.append("evaluate") or [],
    )
    monkeypatch.setattr(
        pipeline_module,
        "queue_price_drop_alerts",
        lambda: calls.append("queue") or 0,
    )

    pipeline_module.run_pipeline(
        queue_alerts=False,
        evaluate_searches=False,
    )

    assert calls == ["load", "snapshot", "score"]


def test_default_pipeline_queues_without_sending_slack(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline_module,
        "load_sample_csv",
        lambda _path: calls.append("load"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "snapshot_current_listings",
        lambda: calls.append("snapshot"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "score_listings_against_market",
        lambda: calls.append("score"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "evaluate_enabled_searches",
        lambda: calls.append("evaluate") or [],
    )
    monkeypatch.setattr(
        pipeline_module,
        "queue_price_drop_alerts",
        lambda: calls.append("queue") or 1,
    )

    pipeline_module.run_pipeline()

    assert calls == ["load", "snapshot", "score", "evaluate", "queue"]

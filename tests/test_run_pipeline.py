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
        "send_price_drop_alerts",
        lambda: calls.append("slack"),
    )

    pipeline_module.run_pipeline(send_alerts=False)

    assert calls == ["load", "snapshot"]

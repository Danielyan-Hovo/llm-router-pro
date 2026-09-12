from src.metrics import COST, LATENCY, REQUESTS, metrics_endpoint


def test_metrics_collectors_have_expected_labels():
    assert LATENCY._labelnames == ("provider", "model")
    assert REQUESTS._labelnames == ("provider", "status")
    assert COST._labelnames == ("provider",)


def test_metrics_record_success_updates_collectors():
    from src.metrics import record_success

    record_success("test-provider", "test-model", 0.25, 1.5)
    assert LATENCY.labels(provider="test-provider", model="test-model")


def test_metrics_record_failure_updates_counter():
    from src.metrics import record_failure

    record_failure("test-provider", "error")
    assert REQUESTS.labels(provider="test-provider", status="error")


def test_metrics_endpoint_is_async_callable():
    assert callable(metrics_endpoint)

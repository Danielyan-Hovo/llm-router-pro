from __future__ import annotations

from src.rate_limit import RateLimiter, RequestLogger


def test_rate_limiter_allows_requests():
    limiter = RateLimiter(max_requests=3, window_seconds=60)
    assert limiter.is_allowed("client_1")
    assert limiter.is_allowed("client_1")
    assert limiter.is_allowed("client_1")
    assert not limiter.is_allowed("client_1")


def test_rate_limiter_remaining():
    limiter = RateLimiter(max_requests=5, window_seconds=60)
    assert limiter.remaining("client_2") == 5
    limiter.is_allowed("client_2")
    assert limiter.remaining("client_2") == 4


def test_rate_limiter_reset():
    limiter = RateLimiter(max_requests=2, window_seconds=60)
    limiter.is_allowed("client_3")
    limiter.is_allowed("client_3")
    assert not limiter.is_allowed("client_3")
    limiter.reset("client_3")
    assert limiter.is_allowed("client_3")


def test_request_logger_structured():
    logger = RequestLogger()
    logger.log_request("POST", "/v1/chat/completions", 200, 45.5, tenant_id="t1", model="gpt-4")
    logs = logger.get_logs()
    assert len(logs) == 1
    assert logs[0]["method"] == "POST"
    assert logs[0]["status_code"] == 200
    assert logs[0]["duration_ms"] == 45.5
    assert logs[0]["tenant_id"] == "t1"
    assert logs[0]["model"] == "gpt-4"


def test_request_logger_limit_and_clear():
    logger = RequestLogger()
    for i in range(150):
        logger.log_request("GET", "/healthz", 200, 1.0)
    assert len(logger.get_logs(limit=100)) == 100
    logger.clear()
    assert len(logger.get_logs()) == 0

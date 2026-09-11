import pytest
from src.metrics import LATENCY, REQUESTS

def test_metrics_export():
    # Mock test for Prometheus metrics
    assert LATENCY is not None

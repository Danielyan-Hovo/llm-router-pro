import hashlib
import hmac

from src.auth import extract_signature, verify_signature


def test_hmac_verification_accepts_valid_signature():
    body = b'{"model":"gpt-4","messages":[]}'
    key = "test-secret"
    signature = hmac.new(key.encode(), body, hashlib.sha256).hexdigest()

    assert verify_signature(body, key, signature)


def test_hmac_verification_rejects_invalid_signature():
    assert not verify_signature(b"payload", "test-secret", "invalid")


def test_hmac_verification_accepts_prefixed_signature():
    body = b"payload"
    key = "test-secret"
    signature = hmac.new(key.encode(), body, hashlib.sha256).hexdigest()

    assert verify_signature(body, key, f"sha256={signature}")


def test_extract_signature_supports_github_header():
    assert extract_signature({"x-hub-signature-256": "sha256=value"}) == "sha256=value"

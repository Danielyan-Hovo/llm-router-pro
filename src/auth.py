import hashlib
import hmac


def verify_signature(body: bytes, api_key: str, signature: str) -> bool:
    if not body or not api_key or not signature:
        return False
    expected = hmac.new(api_key.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.removeprefix("sha256="))


def extract_signature(headers: dict[str, str]) -> str | None:
    return headers.get("x-signature") or headers.get("x-hub-signature-256")

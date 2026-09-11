import hmac
import hashlib
from fastapi import Request, HTTPException

def verify_signature(request: Request, api_key: str, signature: str) -> bool:
    expected = hmac.new(api_key.encode(), request.body(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

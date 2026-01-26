import os
from fastapi import HTTPException, Request, status

API_KEY_HEADER = "x-api-key"

def require_api_key(request: Request) -> None:
    # /health siempre libre
    if request.url.path == "/health":
        return

    # Solo protegemos /v1/*
    if not request.url.path.startswith("/v1/"):
        return

    raw = os.getenv("API_KEYS", "")
    allowed = {k.strip() for k in raw.split(",") if k.strip()}

    provided = request.headers.get(API_KEY_HEADER)

    if not provided:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": {"code": "AUTH_MISSING_API_KEY", "message": "Missing X-API-Key"}},
        )

    if provided not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": {"code": "AUTH_INVALID_API_KEY", "message": "Invalid X-API-Key"}},
        )

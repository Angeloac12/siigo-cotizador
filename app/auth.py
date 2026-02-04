# app/auth.py
import os
import time
import hashlib
import httpx
from fastapi import HTTPException, Request, status

API_KEY_HEADER = "x-api-key"

# cache simple (evita pegarle a Supabase en cada request)
_CACHE_TTL_S = int(os.getenv("API_KEYS_CACHE_TTL_S", "60"))
_cache: dict[str, tuple[float, bool]] = {}  # key_hash -> (expires_at, ok)

def _hash_api_key(secret: str) -> str:
    """
    Genera el mismo hash que guardas en public.api_keys.key_hash (len 64 hex).
    Si defines API_KEY_PEPPER en el backend, el hash será sha256(pepper + secret).
    """
    pepper = os.getenv("API_KEY_PEPPER", "")
    b = (pepper + (secret or "")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def _supabase_allows_api_key(provided: str) -> bool:
    url = os.getenv("SUPABASE_URL")
    srv = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not srv:
        return False  # aún no configurado

    key_hash = _hash_api_key(provided)

    now = time.time()
    hit = _cache.get(key_hash)
    if hit and hit[0] > now:
        return hit[1]

    endpoint = url.rstrip("/") + "/rest/v1/api_keys"
    params = {
        "select": "id",
        "key_hash": f"eq.{key_hash}",
        "is_active": "eq.true",
        "limit": "1",
    }
    headers = {
        "apikey": srv,
        "authorization": f"Bearer {srv}",
        "accept": "application/json",
    }

    try:
        r = httpx.get(endpoint, params=params, headers=headers, timeout=5.0)
        ok = r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) > 0
    except Exception:
        ok = False

    _cache[key_hash] = (now + _CACHE_TTL_S, ok)
    return ok

def require_api_key(request: Request) -> None:
    # health libre (tu endpoint real es /v1/quote-drafts/health)
    if request.url.path.endswith("/health"):
        return

    # Solo protegemos /v1/*
    if not request.url.path.startswith("/v1/"):
        return

    provided = request.headers.get(API_KEY_HEADER)
    if not provided:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": {"code": "AUTH_MISSING_API_KEY", "message": "Missing X-API-Key"}},
        )

    # 1) allowlist por env (dev)
    raw = os.getenv("API_KEYS", "")
    allowed = {k.strip() for k in raw.split(",") if k.strip()}
    if provided in allowed:
        return

    # 2) Supabase api_keys (prod)
    if _supabase_allows_api_key(provided):
        return

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"error": {"code": "AUTH_INVALID_API_KEY", "message": "Invalid X-API-Key"}},
    )

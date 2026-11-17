import math
import os

import httpx


def quote_read_timeout() -> float:
    """Siigo recommends at least 120s; bound overrides to keep waits finite."""
    try:
        value = float(os.getenv("SIIGO_QUOTE_READ_TIMEOUT_SECONDS", "120"))
    except ValueError:
        return 120.0
    return value if math.isfinite(value) and 30 <= value <= 300 else 120.0


def quote_timeout() -> httpx.Timeout:
    return httpx.Timeout(quote_read_timeout(), connect=10.0, write=30.0, pool=10.0)


def quote_proxy_timeout() -> httpx.Timeout:
    # Allow a token refresh/rejected attempt and customer creation, if needed.
    return httpx.Timeout(2 * quote_read_timeout() + 90, connect=10.0, write=30.0, pool=10.0)

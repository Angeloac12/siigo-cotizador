import json
import time
from typing import Callable
from ulid import ULID
from starlette.requests import Request
from starlette.responses import Response

CORR_HEADER = "x-correlation-id"

def new_correlation_id() -> str:
    return f"corr_{ULID()}"

async def request_logger(request: Request, call_next: Callable) -> Response:
    start = time.time()
    corr_id = request.headers.get(CORR_HEADER) or new_correlation_id()
    request.state.correlation_id = corr_id

    response: Response = await call_next(request)

    latency_ms = int((time.time() - start) * 1000)
    log = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": "INFO",
        "correlation_id": corr_id,
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "latency_ms": latency_ms,
    }
    print(json.dumps(log, ensure_ascii=False))
    response.headers["X-Correlation-Id"] = corr_id
    return response

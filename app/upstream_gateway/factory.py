from __future__ import annotations

from typing import Optional

from app.upstream_gateway.siigo import SiigoGateway

_gateway: Optional[SiigoGateway] = None


def get_gateway() -> SiigoGateway:
    global _gateway
    if _gateway is None:
        _gateway = SiigoGateway()
    return _gateway

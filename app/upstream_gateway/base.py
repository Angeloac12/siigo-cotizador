from __future__ import annotations
from typing import Protocol, Optional, Dict, Any, List, TypedDict


class UpstreamGateway(Protocol):
    def find_client(self, identification: str) -> Optional[Dict[str, Any]]:
        ...

    def create_client(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def create_quote(
        self,
        *,
        customer_identification: str,
        branch_office: int,
        document_id: int,
        seller: int,
        items: List[Dict[str, Any]],
        date_iso: str,
    ) -> Dict[str, Any]:
        ...

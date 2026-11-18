from __future__ import annotations
from typing import Protocol, Optional, Dict, Any, List, TypedDict


class UpstreamGateway(Protocol):
    def list_quotes(self, *, identification: str, branch_office: int,
                    created_start: str, created_end: str, page: int = 1) -> Dict[str, Any]:
        ...

    def get_quote(self, quotation_id: str) -> Dict[str, Any]:
        ...

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

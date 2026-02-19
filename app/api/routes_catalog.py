from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Any, Dict, List
from sqlalchemy import text, create_engine
import os

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])

_ENGINE = None
def get_engine():
    global _ENGINE
    if _ENGINE is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL not set")
        _ENGINE = create_engine(db_url, pool_pre_ping=True)
    return _ENGINE

class CatalogSearchIn(BaseModel):
    org_id: str
    q: str = Field(..., min_length=1)
    provider: str = "siigo"
    limit: int = Field(5, ge=1, le=20)

@router.post("/search")
def search_catalog(payload: CatalogSearchIn) -> Dict[str, Any]:
    eng = get_engine()

    sql = text("""
      SELECT
        code, name, description, brand, model, price1, unit,
        similarity(coalesce(search_text,''), unaccent(lower(:q))) AS sim,
        ts_rank(coalesce(search_tsv,''::tsvector), plainto_tsquery('simple', unaccent(lower(:q)))) AS rank
      FROM catalog_products
      WHERE org_id = :org_id AND provider = :provider
      ORDER BY (similarity(coalesce(search_text,''), unaccent(lower(:q))) * 0.7)
             + (ts_rank(coalesce(search_tsv,''::tsvector), plainto_tsquery('simple', unaccent(lower(:q)))) * 0.3)
             DESC,
             code ASC
      LIMIT :limit
    """)

    with eng.connect() as conn:
        rows = conn.execute(sql, {
            "org_id": payload.org_id,
            "provider": payload.provider,
            "q": payload.q,
            "limit": payload.limit,
        }).mappings().all()

    return {"q": payload.q, "results": [dict(r) for r in rows]}

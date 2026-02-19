# app/api/routes_matching.py
import os
from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import text
from app.db_engine import get_engine  # ✅ evita ciclo con app.main
from pydantic import BaseModel


router = APIRouter(prefix="/v1", tags=["matching"])

def _enabled() -> bool:
    return (os.getenv("ENABLE_MATCHING") or "").lower() in ("1", "true", "yes", "y", "on")

@router.post("/drafts/{draft_id}/match")
def match_draft_items(draft_id: str, payload: dict = Body(...)):
    # Feature flag (no romper nada)
    if not _enabled():
        raise HTTPException(status_code=404, detail={"code": "MATCHING_DISABLED"})

    org_id = (payload.get("org_id") or "").strip()
    provider = (payload.get("provider") or "siigo").strip()
    limit = int(payload.get("limit") or 5)
    apply = bool(payload.get("apply") or False)  # ✅ si true: persiste item_code en draft_items

    if not org_id:
        raise HTTPException(status_code=400, detail={"code": "MISSING_ORG_ID"})

    eng = get_engine()

    # 1) Traer items del draft
    with eng.connect() as conn:
        items = conn.execute(
            text("""
                SELECT line_index, COALESCE(NULLIF(description,''), raw_text) AS q
                FROM draft_items
                WHERE draft_id=:draft_id
                ORDER BY line_index
                LIMIT 200
            """),
            {"draft_id": draft_id},
        ).mappings().all()

    if not items:
        raise HTTPException(status_code=404, detail={"code": "DRAFT_HAS_NO_ITEMS"})

    results_out = []

    # 2) Match por item + (opcional) persistir selección
    with eng.begin() as conn:
        for it in items:
            q = (it["q"] or "").strip()
            if not q:
                continue

            rows = conn.execute(
                text("""
                    SELECT code, name, description, brand, model, price1, unit,
                           similarity(search_text, unaccent(lower(:q))) AS sim,
                           ts_rank(search_tsv, plainto_tsquery('simple', unaccent(lower(:q)))) AS rank
                    FROM catalog_products
                    WHERE org_id=:org_id AND provider=:provider
                    ORDER BY (
                        ts_rank(search_tsv, plainto_tsquery('simple', unaccent(lower(:q))))*2
                        + similarity(search_text, unaccent(lower(:q)))
                    ) DESC
                    LIMIT :limit
                """),
                {"org_id": org_id, "provider": provider, "q": q, "limit": limit},
            ).mappings().all()

            if not rows:
                continue

            best = rows[0]
            selected = {
                "code": str(best["code"]),
                "name": best["name"],
                "sim": float(best["sim"] or 0),
                "rank": float(best["rank"] or 0),
            }

            # ✅ Persistir en draft_items si apply=true
            if apply:
                conn.execute(
                    text("""
                        UPDATE draft_items
                        SET item_code=:code,
                            item_name=:name,
                            match_sim=:sim,
                            match_rank=:rank,
                            updated_at=now()
                        WHERE draft_id=:draft_id AND line_index=:line_index
                    """),
                    {
                        "draft_id": draft_id,
                        "line_index": int(it["line_index"]),
                        "code": selected["code"],
                        "name": selected["name"],
                        "sim": selected["sim"],
                        "rank": selected["rank"],
                    },
                )

            results_out.append({
                "line_index": int(it["line_index"]),
                "q": q,
                "selected": selected,
                "candidates": [dict(r) for r in rows],
            })

    return {
        "draft_id": draft_id,
        "org_id": org_id,
        "provider": provider,
        "apply": apply,
        "items": results_out,
    }



class UpdateDescriptionBody(BaseModel):
    description: str

@router.patch("/drafts/{draft_id}/items/{line_index}/description")
def update_item_description(draft_id: str, line_index: int, payload: UpdateDescriptionBody):
    desc = (payload.description or "").strip()
    if not desc:
        raise HTTPException(status_code=400, detail={"code": "MISSING_DESCRIPTION"})

    eng = get_engine()

    with eng.begin() as conn:
        # 1) asegurar que el item exista
        row = conn.execute(text("""
            SELECT line_index, COALESCE(item_code,'') AS item_code, COALESCE(item_name,'') AS item_name
            FROM draft_items
            WHERE draft_id=:draft_id AND line_index=:line_index
            LIMIT 1
        """), {"draft_id": draft_id, "line_index": line_index}).mappings().first()

        if not row:
            raise HTTPException(status_code=404, detail={"code": "ITEM_NOT_FOUND"})

        # 2) guardar override (esto es lo que debe priorizar quote/commit)
        conn.execute(text("""
            INSERT INTO draft_item_selections(
                draft_id, line_index, provider,
                selected_code, selected_name,
                chosen_by, description_override, updated_at
            )
            VALUES(
                :draft_id, :line_index, 'siigo',
                NULLIF(:item_code,''), NULLIF(:item_name,''),
                'user', :desc, now()
            )
            ON CONFLICT (draft_id, line_index) DO UPDATE SET
                description_override = EXCLUDED.description_override,
                chosen_by = 'user',
                updated_at = now()
        """), {
            "draft_id": draft_id,
            "line_index": int(line_index),
            "item_code": str(row.get("item_code") or ""),
            "item_name": str(row.get("item_name") or ""),
            "desc": desc,
        })

        # 3) opcional: también actualiza la description visible del draft_items
        conn.execute(text("""
            UPDATE draft_items
            SET description=:desc, updated_at=now()
            WHERE draft_id=:draft_id AND line_index=:line_index
        """), {"draft_id": draft_id, "line_index": int(line_index), "desc": desc})

    return {"draft_id": draft_id, "line_index": line_index, "description": desc, "saved_as": "description_override"}

from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import text
from app.db_engine import get_engine

router = APIRouter(prefix="/v1", tags=["overrides"])

@router.put("/drafts/{draft_id}/items/{line_index}/description")
def set_description_override(draft_id: str, line_index: int, payload: dict = Body(...)):
    desc = (payload.get("description") or "").strip()
    if not desc:
        raise HTTPException(status_code=400, detail={"code": "MISSING_DESCRIPTION"})

    eng = get_engine()
    with eng.begin() as conn:
        # upsert override
        conn.execute(text("""
            INSERT INTO draft_item_selections (draft_id, line_index, provider, chosen_by, description_override)
            VALUES (:draft_id, :line_index, 'siigo', 'user', :desc)
            ON CONFLICT (draft_id, line_index) DO UPDATE SET
              description_override = EXCLUDED.description_override,
              chosen_by = 'user',
              updated_at = now()
        """), {"draft_id": draft_id, "line_index": int(line_index), "desc": desc})

    return {"draft_id": draft_id, "line_index": int(line_index), "description_override": desc}

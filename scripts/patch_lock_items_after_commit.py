from pathlib import Path

path = Path("app/main.py")
txt = path.read_text(encoding="utf-8")

OLD = """        exists = conn.execute(text("SELECT id FROM drafts WHERE id = :id"), {"id": draft_id}).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Draft not found")
"""

NEW = """        draft_row = conn.execute(
            text("SELECT id, status, warnings_json FROM drafts WHERE id = :id"),
            {"id": draft_id},
        ).mappings().first()

        if not draft_row:
            raise HTTPException(status_code=404, detail="Draft not found")

        w = draft_row.get("warnings_json") or {}
        has_quote = isinstance(w, dict) and w.get("siigo_quote_response")

        if draft_row.get("status") == "COMMITTED" or has_quote:
            raise HTTPException(status_code=409, detail="Draft is COMMITTED. Create a new draft to edit items.")
"""

if OLD not in txt:
    raise SystemExit("❌ No encontré el bloque OLD exacto en replace_draft_items(). Mándame el bloque actual de esa parte.")

txt = txt.replace(OLD, NEW, 1)

# endurece idempotencia: si ya hay siigo_quote_response, tratar como committed aunque status esté mal
IDEMP_OLD = 'if draft.get("status") == "COMMITTED" and not body.dry_run:'
IDEMP_NEW = 'if (draft.get("status") == "COMMITTED" or (isinstance(draft.get("warnings_json"), dict) and draft.get("warnings_json").get("siigo_quote_response"))) and not body.dry_run:'

if IDEMP_OLD not in txt:
    raise SystemExit("❌ No encontré la línea de idempotencia en commit_quote().")

txt = txt.replace(IDEMP_OLD, IDEMP_NEW, 1)

path.write_text(txt, encoding="utf-8")
print("✅ Patch aplicado")

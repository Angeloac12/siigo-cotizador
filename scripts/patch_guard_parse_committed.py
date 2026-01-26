from pathlib import Path
import re
import sys

TARGET = Path("app/main.py")

src = TARGET.read_text(encoding="utf-8")

original = src

# (A) Asegura que el SELECT del draft traiga status (si existe el patrón exacto)
src = src.replace(
    'SELECT id, stored_path, original_filename FROM drafts WHERE id = :id',
    'SELECT id, status, stored_path, original_filename FROM drafts WHERE id = :id',
)

# (B) Inserta guard de COMMITTED justo después del "Draft not found"
# Lo metemos ANTES del check de items para que el mensaje sea el correcto.
pattern = re.compile(
    r'(if not draft:\s*\n\s*raise HTTPException\(\s*status_code\s*=\s*404\s*,\s*detail\s*=\s*"Draft not found"\s*\)\s*\n)',
    re.MULTILINE
)

guard = r"""\1
    # Guard: si ya fue enviado a Siigo, no se puede re-parsear (evita sobrescrituras)
    if (draft.get("status") or "").upper() == "COMMITTED":
        raise HTTPException(
            status_code=409,
            detail="Draft is COMMITTED. Create a new draft to parse.",
        )
"""

src, n = pattern.subn(guard, src, count=1)

if n == 0:
    print("ERROR: No pude insertar el guard. No encontré el bloque 'Draft not found' en parse_draft.")
    print("TIP: pega aquí el bloque de tu endpoint /v1/drafts/{draft_id}/parse (30-60 líneas) y lo ajusto.")
    sys.exit(1)

if src == original:
    print("WARNING: El archivo no cambió (tal vez ya estaba aplicado el cambio).")
else:
    TARGET.write_text(src, encoding="utf-8")
    print("OK: Guard COMMITTED insertado en /parse")

from pathlib import Path
import re

path = Path("app/main.py")
s = path.read_text(encoding="utf-8")

pattern = r'text\(\s*["\']UPDATE drafts SET status=\'PARSED\', updated_at=now\(\) WHERE id=:id["\']\s*\)'
replacement = """text(\"\"\"
UPDATE drafts
SET status = CASE WHEN status='COMMITTED' THEN 'COMMITTED' ELSE 'PARSED' END,
    updated_at=now()
WHERE id=:id
\"\"\")"""

new_s, n = re.subn(pattern, replacement, s, count=1)

if n == 0:
    print("NO CAMBIOS: no encontré el UPDATE viejo exacto en app/main.py")
    print("TIP: corre: grep -n \"UPDATE drafts SET status='PARSED'\" -n app/main.py")
else:
    path.write_text(new_s, encoding="utf-8")
    print("OK: UPDATE de replace_draft_items blindado (CASE WHEN COMMITTED).")

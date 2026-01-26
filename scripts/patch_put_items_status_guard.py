from pathlib import Path

p = Path("app/main.py")
s = p.read_text(encoding="utf-8")

old = "UPDATE drafts SET status='PARSED', updated_at=now() WHERE id=:id"
new = "UPDATE drafts SET status = CASE WHEN status='COMMITTED' THEN 'COMMITTED' ELSE 'PARSED' END, updated_at=now() WHERE id=:id"

if old in s:
    s = s.replace(old, new)
    p.write_text(s, encoding="utf-8")
    print("OK: patched PUT /items status update")
else:
    print("OK: nothing to patch (old string not found)")


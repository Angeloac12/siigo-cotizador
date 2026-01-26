import re
from pathlib import Path

p = Path("app/main.py")
s = p.read_text(encoding="utf-8")

# 1) Asegura import
import_line = "from app.api.routes_quote_drafts import router as quote_drafts_router\n"
if import_line not in s:
    # lo mete después de los imports iniciales (primera línea en blanco doble)
    m = re.search(r"\n\n", s)
    if not m:
        s = import_line + s
    else:
        s = s[: m.end()] + import_line + s[m.end() :]

# 2) Inserta include_router justo después de app = FastAPI(...)
m = re.search(r"^\s*app\s*=\s*FastAPI\s*\(", s, flags=re.M)
if not m:
    raise SystemExit("No encontré 'app = FastAPI(' en app/main.py")

start = m.end() - 1  # apunta al '('
depth = 0
end = None
for i in range(start, len(s)):
    ch = s[i]
    if ch == "(":
        depth += 1
    elif ch == ")":
        depth -= 1
        if depth == 0:
            end = i + 1
            break

if end is None:
    raise SystemExit("No pude encontrar el cierre de FastAPI(...)")

line_end = s.find("\n", end)
if line_end == -1:
    line_end = end

needle = "app.include_router(quote_drafts_router)\n"
if needle not in s:
    s = s[: line_end + 1] + needle + s[line_end + 1 :]

p.write_text(s, encoding="utf-8")
print("OK: main.py actualizado (import + include_router).")

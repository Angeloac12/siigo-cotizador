import re
from pathlib import Path

p = Path("app/main.py")
s = p.read_text(encoding="utf-8").splitlines(True)

needle = "app.include_router(quote_drafts_router)\n"

# 1) quitar cualquier include_router existente (mal ubicado)
s = [line for line in s if line != needle]

# 2) encontrar dónde se define app = FastAPI(...)
idx = None
for i, line in enumerate(s):
    if re.match(r"^\s*app\s*=\s*FastAPI\s*\(", line):
        idx = i
        break
if idx is None:
    raise SystemExit("No encontré la línea: app = FastAPI(")

# 3) encontrar el cierre del FastAPI(...) para insertar justo después
text = "".join(s)
start = text.find("app = FastAPI", 0)
start_paren = text.find("(", start)
depth = 0
end_pos = None
for j in range(start_paren, len(text)):
    ch = text[j]
    if ch == "(":
        depth += 1
    elif ch == ")":
        depth -= 1
        if depth == 0:
            end_pos = j + 1
            break
if end_pos is None:
    raise SystemExit("No pude encontrar el cierre de FastAPI(...)")

# línea donde termina el bloque FastAPI(...)
line_end = text.find("\n", end_pos)
if line_end == -1:
    line_end = end_pos

# convertir posición a índice de línea
pos = 0
insert_after_line = 0
for i, line in enumerate(s):
    pos += len(line)
    if pos >= line_end:
        insert_after_line = i
        break

# 4) insertar include_router después de app creado
s.insert(insert_after_line + 1, needle)

p.write_text("".join(s), encoding="utf-8")
print("OK: include_router movido debajo de app = FastAPI(...)")

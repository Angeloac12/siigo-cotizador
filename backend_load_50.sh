#!/bin/bash
set -u

API="https://siigo-cotizador.onrender.com/v1/quote-drafts/process"
KEY="dev_key_123"

# Archivos: primero intenta local (repo), si no existe usa ~/qa_proq
FAST_FILE="$(pwd)/rfq_hard.txt"
SLOW_FILE="$(pwd)/cotizacion_test.txt"

if [ ! -f "$FAST_FILE" ]; then FAST_FILE="$HOME/qa_proq/rfq_hard.txt"; fi
if [ ! -f "$SLOW_FILE" ]; then SLOW_FILE="$HOME/qa_proq/cotizacion_test.txt"; fi

OUTDIR="$HOME/qa_proq/backend_load_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

CSV="$OUTDIR/results.csv"
echo "i,http,total_s,file,items_created,warnings_count,warnings,error_code,cid" > "$CSV"

echo "OUTDIR=$OUTDIR"
echo "FAST_FILE=$FAST_FILE"
echo "SLOW_FILE=$SLOW_FILE"
echo

for i in $(seq 1 50); do
  # 25 fast + 25 slow (así ves claramente diferencias de latencia)
  if [ "$i" -le 25 ]; then
    FILE="$FAST_FILE"
  else
    FILE="$SLOW_FILE"
  fi

  CID="backend_load_${i}_$(date +%Y%m%d_%H%M%S)"
  RESP="$OUTDIR/resp_${i}.json"

  ec=0
  stats="$(curl -sS --max-time 140 -o "$RESP" -w "%{http_code} %{time_total}" \
    -X POST "$API" \
    -H "X-API-Key: $KEY" \
    -H "X-Correlation-Id: $CID" \
    -F "file=@${FILE};type=text/plain" \
    -F "customer_identification=900123456" \
    -F "document_id=19026" \
    -F "seller=1298" \
    -F "branch_office=0" \
    -F "default_price=1" \
    -F "dry_run=true" \
    -F "create_customer_if_missing=false" \
  )" || ec=$?

  if [ "$ec" -ne 0 ]; then
    http="000"
    total="0"
  else
    http="$(echo "$stats" | awk '{print $1}')"
    total="$(echo "$stats" | awk '{print $2}')"
  fi

  parsed="$(python3 - <<'PY'
import json, sys, os
path=os.environ.get("RESP","")
err_code=""
items_created=""
warnings=""
wcount=0
try:
    d=json.load(open(path,"r",encoding="utf-8"))
    if "detail" in d:
        detail=d.get("detail") or {}
        err_code=str(detail.get("code") or "ERROR")
        parse=(detail.get("parse") or {})
    else:
        parse=(d.get("parse") or {})
    items_created=parse.get("items_created","")
    w=parse.get("warnings") or []
    warnings="|".join(map(str,w)) if w else ""
    wcount=len(w) if w else 0
except Exception:
    err_code="NON_JSON"
print(f"{items_created},{wcount},{warnings},{err_code}")
PY
)"
  items_created="$(echo "$parsed" | cut -d, -f1)"
  wcount="$(echo "$parsed" | cut -d, -f2)"
  warnings="$(echo "$parsed" | cut -d, -f3)"
  err_code="$(echo "$parsed" | cut -d, -f4)"

  echo "${i},${http},${total},${FILE},${items_created},${wcount},${warnings},${err_code},${CID}" >> "$CSV"

  echo "i=${i} HTTP=${http} total=${total}s ec=${ec} file=$(basename "$FILE") cid=$CID"
done

echo
echo "DONE. CSV: $CSV"

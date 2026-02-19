#!/usr/bin/env bash
set -euo pipefail

URL="https://siigo-cotizador.onrender.com/v1/quote-drafts/process"
API_KEY="dev_key_123"

CUSTOMER="900123456"
DOC_ID="19026"
SELLER="1298"
BRANCH="0"
DEFAULT_PRICE="1"
DRY_RUN="true"
CREATE_CUSTOMER="false"

# args: N P FAST_FILE SLOW_FILE OUTDIR
N="${1:-50}"                      # total requests
P="${2:-5}"                       # concurrency
FAST_FILE="${3:-./rfq_hard.txt}"  # archivo "rápido"
SLOW_FILE="${4:-./cotizacion_test.txt}"  # archivo "lento" / grande
OUTDIR="${5:-$HOME/qa_proq/backend_load_$(date +%Y%m%d_%H%M%S)}"

mkdir -p "$OUTDIR/outs" "$OUTDIR/meta"

export URL API_KEY CUSTOMER DOC_ID SELLER BRANCH DEFAULT_PRICE DRY_RUN CREATE_CUSTOMER FAST_FILE SLOW_FILE OUTDIR

run_one() {
  local i="$1"
  local FILE TAG CID OUT META RES

  # 20% lento: cada 5ta petición
  if (( i % 5 == 0 )); then
    FILE="$SLOW_FILE"
    TAG="slow"
  else
    FILE="$FAST_FILE"
    TAG="fast"
  fi

  CID="backend_load_${TAG}_${i}_$(date +%Y%m%d_%H%M%S)"
  OUT="$OUTDIR/outs/out_${i}.json"
  META="$OUTDIR/meta/meta_${i}.txt"

  RES="$(curl -s -o "$OUT" -w "HTTP=%{http_code} total=%{time_total}\n" \
    -X POST "$URL" \
    -H "X-API-Key: $API_KEY" \
    -H "X-Correlation-Id: $CID" \
    -F "file=@${FILE};type=text/plain" \
    -F "customer_identification=${CUSTOMER}" \
    -F "document_id=${DOC_ID}" \
    -F "seller=${SELLER}" \
    -F "branch_office=${BRANCH}" \
    -F "default_price=${DEFAULT_PRICE}" \
    -F "dry_run=${DRY_RUN}" \
    -F "create_customer_if_missing=${CREATE_CUSTOMER}" \
    || echo "HTTP=000 total=0")"

  echo "i=$i file=$FILE tag=$TAG cid=$CID $RES" > "$META"
  echo "i=$i $RES tag=$TAG"
}
export -f run_one

echo "OUTDIR=$OUTDIR"
echo "N=$N concurrency=$P"
echo "FAST_FILE=$FAST_FILE"
echo "SLOW_FILE=$SLOW_FILE"

seq 1 "$N" | xargs -n1 -P "$P" -I{} bash -lc 'run_one "$@"' _ {}

python3 - <<'PY'
import glob, json, re, csv, os
outdir=os.environ["OUTDIR"]
meta_files=sorted(glob.glob(os.path.join(outdir,"meta","meta_*.txt")))

rows=[]
for mf in meta_files:
    s=open(mf,encoding="utf-8").read().strip()
    kv=dict(re.findall(r'(\w+)=([^\s]+)', s))
    i=int(kv.get("i","0"))
    file=kv.get("file","")
    cid=kv.get("cid","")
    http=int(kv.get("HTTP","0"))
    total=float(kv.get("total","nan"))

    out_path=os.path.join(outdir,"outs",f"out_{i}.json")
    items_created=""
    warnings=""
    wcount=0
    err_code=""

    try:
        d=json.load(open(out_path,encoding="utf-8"))
        if "detail" in d:
            det=d["detail"]
            if isinstance(det,dict):
                err_code=str(det.get("code") or "")
                parse=(det.get("parse") or {})
                items_created=parse.get("items_created","")
                w=parse.get("warnings") or []
                warnings="|".join(map(str,w)) if w else ""
                wcount=len(w) if w else 0
        else:
            parse=d.get("parse") or {}
            items_created=parse.get("items_created","")
            w=parse.get("warnings") or []
            warnings="|".join(map(str,w)) if w else ""
            wcount=len(w) if w else 0
    except Exception:
        err_code="NON_JSON"

    rows.append([i,http,total,file,items_created,wcount,warnings,err_code,cid])

rows.sort(key=lambda r:r[0])
csv_path=os.path.join(outdir,"results.csv")
with open(csv_path,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f)
    w.writerow(["i","http","total_s","file","items_created","warnings_count","warnings","error_code","cid"])
    w.writerows(rows)

print("WROTE", csv_path, "rows", len(rows))
PY

echo
echo "OK. Results: $OUTDIR/results.csv"

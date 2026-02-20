import os
import re
import json
import unicodedata
from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import text
from app.db_engine import get_engine

router = APIRouter(prefix="/v1", tags=["matching"])

def _enabled() -> bool:
    return (os.getenv("ENABLE_MATCHING") or "").lower() in ("1", "true", "yes", "y", "on")


# =========================
# Multi-tenant config
# =========================
_DEFAULT_MATCH_CONFIG = {
    "categories": {
        "cable": ["cable", "alambre", "conductor"],
        "breaker": ["breaker", "interruptor", "termomagnetico", "termomagnético"],
    },
    "keywords": {
        "insulated": ["aislado", "aislada", "thhn", "thw", "thhw", "xlpe", "pvc", "hffr", "libre halogenos", "libre halógenos"],
        "bare": ["desnudo", "desnuda", "bare"],
        "roll": ["rollo", "rollos", "rol"],  # "rol" se matchea por palabra completa (no dentro de control)
    },
    # Términos “muy específicos”: si están en el candidato pero NO en el query => penaliza
    "avoid_terms": {
        "cable": ["instrumentacion", "instrumentación", "control", "soldador", "vehicular"],
        "breaker": ["transferencia", "automatica", "automática"],
    },
    # Si el user pide "aislado" y el candidato tiene estos estándares => bonus
    "preferred_terms": {
        "cable": ["thhn", "thw", "thhw", "hffr"],
    },
    "weights": {
        "awg_match_bonus": 2.5,
        "awg_mismatch_penalty": 6.0,
        "awg_missing_penalty": 1.2,

        "amp_match_bonus": 2.0,
        "amp_mismatch_penalty": 4.0,
        "amp_missing_penalty": 0.6,

        "want_insulated_bonus": 1.2,
        "want_insulated_bare_penalty": 4.0,

        "want_bare_bonus": 1.2,
        "want_bare_insulated_penalty": 4.0,

        "want_roll_bonus": 0.6,

        "avoid_term_penalty": 2.4,        # <= clave para bajar “control/instrumentación”
        "preferred_term_bonus": 0.9,      # empuja THHN/THW arriba cuando piden aislado
    },
    "recall_multiplier": 8,
}

_CONFIG_BY_ORG = {}
try:
    _raw = os.getenv("MATCHING_CONFIG_BY_ORG_JSON") or ""
    if _raw.strip():
        _CONFIG_BY_ORG = json.loads(_raw)
except Exception:
    _CONFIG_BY_ORG = {}

def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(out.get(k), dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def get_match_config(org_id: str) -> dict:
    cfg = dict(_DEFAULT_MATCH_CONFIG)
    override = _CONFIG_BY_ORG.get(org_id) if isinstance(_CONFIG_BY_ORG, dict) else None
    if isinstance(override, dict):
        cfg = _deep_merge(cfg, override)
    return cfg


# =========================
# Normalización (acentos)
# =========================
def _fold(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().strip()

def _has_any(text: str, words: list[str]) -> bool:
    t = _fold(text)
    for w in (words or []):
        wl = _fold(w)
        if not wl:
            continue
        # si es keyword corta => match por palabra completa (evita "contROl" con "rol")
        if len(wl) <= 3:
            if re.search(rf"\b{re.escape(wl)}\b", t):
                return True
        else:
            if wl in t:
                return True
    return False


# =========================
# Spec extraction
# =========================
_RE_AWG_NUM = re.compile(r"(?:#\s*(\d{1,2})\b|\bno\.?\s*(\d{1,2})\b|\bn\s*(\d{1,2})\b|\b(\d{1,2})\s*awg\b|\bawg\s*(\d{1,2})\b)", re.I)
_RE_AWG_OUGHT = re.compile(r"\b([1-4])\s*[/\-]\s*0\b", re.I)
_RE_AMP = re.compile(r"\b(\d{1,4})\s*a\b|\b(\d{1,4})\s*amp(?:s)?\b", re.I)

def _extract_awg_any(text_in: str) -> str | None:
    t = _fold(text_in)
    m0 = _RE_AWG_OUGHT.search(t)
    if m0:
        return f"{m0.group(1)}/0"
    m = _RE_AWG_NUM.search(t)
    if m:
        g = next((x for x in m.groups() if x), None)
        if g:
            return str(int(g))
    return None

def _extract_amp_any(text_in: str) -> str | None:
    t = _fold(text_in)
    m = _RE_AMP.search(t)
    if m:
        g = next((x for x in m.groups() if x), None)
        if g:
            return str(int(g))
    return None

def _detect_category(q: str, cfg: dict) -> str | None:
    ql = _fold(q)
    cats = (cfg.get("categories") or {})
    for cat, words in cats.items():
        for w in (words or []):
            wl = _fold(w)
            if wl and wl in ql:
                return cat
    return None

def _extract_specs(q: str, cfg: dict) -> dict:
    specs = {}
    specs["cat"] = _detect_category(q, cfg)

    awg = _extract_awg_any(q)
    if awg:
        specs["awg"] = awg

    amp = _extract_amp_any(q)
    if amp:
        specs["amp"] = amp

    kws = cfg.get("keywords") or {}
    specs["want_insulated"] = _has_any(q, kws.get("insulated", []))
    specs["want_bare"] = _has_any(q, kws.get("bare", []))
    specs["want_roll"] = _has_any(q, kws.get("roll", []))

    if specs["want_insulated"] and specs["want_bare"]:
        specs["want_insulated"] = False
        specs["want_bare"] = False

    return specs


def _candidate_text(row: dict) -> str:
    return " ".join([
        str(row.get("name") or ""),
        str(row.get("description") or ""),
        str(row.get("brand") or ""),
        str(row.get("model") or ""),
    ]).strip()

def _candidate_flags(row: dict, cfg: dict) -> dict:
    txt = _candidate_text(row)
    kws = cfg.get("keywords") or {}
    return {
        "awg": _extract_awg_any(txt),
        "amp": _extract_amp_any(txt),
        "has_insulated": _has_any(txt, kws.get("insulated", [])),
        "has_bare": _has_any(txt, kws.get("bare", [])),
        "has_roll": _has_any(txt, kws.get("roll", [])),
        "txt_fold": _fold(txt),
    }

def _spec_adjust(specs: dict, cand_flags: dict, cfg: dict, q_base: str) -> float:
    w = (cfg.get("weights") or {})
    adj = 0.0

    # AWG
    if specs.get("awg"):
        if cand_flags.get("awg") == specs["awg"]:
            adj += float(w.get("awg_match_bonus", 0))
        elif cand_flags.get("awg") is None:
            adj -= float(w.get("awg_missing_penalty", 0))
        else:
            adj -= float(w.get("awg_mismatch_penalty", 0))

    # AMP
    if specs.get("amp"):
        if cand_flags.get("amp") == specs["amp"]:
            adj += float(w.get("amp_match_bonus", 0))
        elif cand_flags.get("amp") is None:
            adj -= float(w.get("amp_missing_penalty", 0))
        else:
            adj -= float(w.get("amp_mismatch_penalty", 0))

    # aislado vs desnudo
    if specs.get("want_insulated"):
        if cand_flags.get("has_bare"):
            adj -= float(w.get("want_insulated_bare_penalty", 0))
        if cand_flags.get("has_insulated"):
            adj += float(w.get("want_insulated_bonus", 0))

    if specs.get("want_bare"):
        if cand_flags.get("has_insulated"):
            adj -= float(w.get("want_bare_insulated_penalty", 0))
        if cand_flags.get("has_bare"):
            adj += float(w.get("want_bare_bonus", 0))

    # rollo
    if specs.get("want_roll") and cand_flags.get("has_roll"):
        adj += float(w.get("want_roll_bonus", 0))

    # penaliza términos extra (control/instrumentación/etc) si el query no los pidió
    cat = specs.get("cat")
    avoid = (cfg.get("avoid_terms") or {}).get(cat, []) if cat else []
    qf = _fold(q_base)
    cf = cand_flags.get("txt_fold") or ""
    for term in avoid:
        tf = _fold(term)
        if tf and tf in cf and tf not in qf:
            adj -= float(w.get("avoid_term_penalty", 0))

    # si pidió aislado, preferimos estándares eléctricos típicos (THHN/THW/THHW/HFFR)
    if specs.get("want_insulated"):
        pref = (cfg.get("preferred_terms") or {}).get(cat, []) if cat else []
        for term in pref:
            tf = _fold(term)
            if tf and tf in cf:
                adj += float(w.get("preferred_term_bonus", 0))
                break

    return adj


# =========================
# SQL recall (solo recall)
# =========================
_SQL_RECALL = """
SELECT
  code, name, description, brand, model, price1, unit,
  similarity(search_text, unaccent(lower(:q))) AS sim,
  word_similarity(unaccent(lower(name)), unaccent(lower(:q))) AS wsim,
  ts_rank(search_tsv, websearch_to_tsquery('simple', unaccent(lower(:q)))) AS rank,
  (
    ts_rank(search_tsv, websearch_to_tsquery('simple', unaccent(lower(:q)))) * 2
    + word_similarity(unaccent(lower(name)), unaccent(lower(:q))) * 2
    + similarity(search_text, unaccent(lower(:q)))
  ) AS score_base
FROM catalog_products
WHERE org_id=:org_id AND provider=:provider
{extra_where}
ORDER BY score_base DESC
LIMIT :fetch_limit
"""


@router.post("/drafts/{draft_id}/match")
def match_draft_items(draft_id: str, payload: dict = Body(...)):
    if not _enabled():
        raise HTTPException(status_code=404, detail={"code": "MATCHING_DISABLED"})

    org_id = (payload.get("org_id") or "").strip()
    provider = (payload.get("provider") or "siigo").strip()
    limit = int(payload.get("limit") or 5)
    apply = bool(payload.get("apply") or False)

    if not org_id:
        raise HTTPException(status_code=400, detail={"code": "MISSING_ORG_ID"})

    cfg = get_match_config(org_id)
    recall_mult = int(cfg.get("recall_multiplier") or 8)
    fetch_limit = max(limit * recall_mult, limit)

    eng = get_engine()

    with eng.connect() as conn:
        items = conn.execute(
            text("""
                SELECT line_index,
                       COALESCE(NULLIF(description,''), raw_text) AS q,
                       raw_text
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

    with eng.begin() as conn:
        for it in items:
            q_base = (it["q"] or "").strip()
            raw_text = (it.get("raw_text") or "").strip()
            if not q_base:
                continue

            specs = _extract_specs(q_base, cfg)

            q_enriched = q_base
            if specs.get("cat") == "breaker" and specs.get("amp"):
                q_enriched = f"breaker {specs['amp']}A {q_base}"
            elif specs.get("cat") == "cable" and specs.get("awg"):
                q_enriched = f"cable {specs['awg']} awg {q_base}"

            # si raw trae keywords y no están en q_enriched, agregamos 1 keyword (solo recall)
            all_kws = (cfg.get("keywords", {}).get("insulated", [])
                      + cfg.get("keywords", {}).get("bare", [])
                      + cfg.get("keywords", {}).get("roll", []))
            for kw in all_kws:
                if kw and _fold(kw) in _fold(raw_text) and _fold(kw) not in _fold(q_enriched):
                    q_enriched = f"{q_enriched} {kw}"
                    break

            extra = []
            params = {
                "org_id": org_id,
                "provider": provider,
                "q": q_enriched,
                "fetch_limit": fetch_limit,
            }

            if specs.get("cat"):
                extra.append("search_text ILIKE :cat_like")
                params["cat_like"] = f"%{specs['cat']}%"

            extra_where = ""
            if extra:
                extra_where = " AND " + " AND ".join(extra)

            sql = _SQL_RECALL.format(extra_where=extra_where)
            rows = conn.execute(text(sql), params).mappings().all()
            if not rows:
                continue

            reranked = []
            for r in rows:
                rdict = dict(r)
                flags = _candidate_flags(rdict, cfg)
                adj = _spec_adjust(specs, flags, cfg, q_base=q_base)
                score_base = float(rdict.get("score_base") or 0)
                score_final = score_base + float(adj)

                rdict["specs_candidate"] = {k: v for k, v in flags.items() if k != "txt_fold"}
                rdict["score_final"] = score_final
                reranked.append(rdict)

            reranked.sort(key=lambda x: float(x.get("score_final") or 0), reverse=True)
            top = reranked[:limit]
            best = top[0]

            selected = {
                "code": str(best["code"]),
                "name": best.get("name"),
                "sim": float(best.get("sim") or 0),
                "rank": float(best.get("rank") or 0),
                "score_base": float(best.get("score_base") or 0),
                "score_final": float(best.get("score_final") or 0),
            }

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
                "q": q_enriched,
                "selected": selected,
                "candidates": [
                    {
                        "code": str(x.get("code")),
                        "name": x.get("name"),
                        "price1": x.get("price1"),
                        "unit": x.get("unit"),
                        "sim": float(x.get("sim") or 0),
                        "wsim": float(x.get("wsim") or 0),
                        "rank": float(x.get("rank") or 0),
                        "score_base": float(x.get("score_base") or 0),
                        "score_final": float(x.get("score_final") or 0),
                        "specs_candidate": x.get("specs_candidate"),
                    }
                    for x in top
                ],
                "specs": specs or None,
                "warnings": None,
            })

    return {
        "draft_id": draft_id,
        "org_id": org_id,
        "provider": provider,
        "apply": apply,
        "items": results_out,
    }

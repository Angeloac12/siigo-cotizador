from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional, List

import httpx
from fastapi import HTTPException

from app.services.siigo_timeouts import quote_timeout


logger = logging.getLogger(__name__)


def _jwt_exp(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        return int(data.get("exp") or 0)
    except Exception:
        return 0


class SiigoGateway:
    def __init__(self):
        self.enabled = os.getenv("SIIGO_ENABLED", "false").lower() in ("1", "true", "yes")
        self.base_url = (os.getenv("SIIGO_BASE_URL", "https://api.siigo.com/v1") or "").rstrip("/")
        self.signin_url = os.getenv("SIIGO_SIGNIN_URL", "")
        self.partner_id = os.getenv("SIIGO_PARTNER_ID", "")
        self.api_user = os.getenv("SIIGO_API_USER", "")
        self.access_key = os.getenv("SIIGO_ACCESS_KEY", "")

        self._token: Optional[str] = None
        self._token_exp: int = 0
        self._lock = threading.Lock()

    def _invalidate_token(self):
        with self._lock:
            self._token = None
            self._token_exp = 0

    def _get_token(self) -> str:
        if not self.enabled:
            raise HTTPException(status_code=501, detail="SIIGO integration disabled. Set SIIGO_ENABLED=true.")

        if not self.api_user or not self.access_key:
            raise HTTPException(status_code=500, detail="Missing SIIGO_API_USER / SIIGO_ACCESS_KEY env vars.")
        if not self.signin_url:
            raise HTTPException(status_code=500, detail="Missing SIIGO_SIGNIN_URL env var.")
        if not self.partner_id:
            raise HTTPException(status_code=500, detail="Missing SIIGO_PARTNER_ID env var.")

        now = int(time.time())

        with self._lock:
            if self._token and now < (self._token_exp - 30):
                return self._token

            try:
                r = httpx.post(
                    self.signin_url,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Partner-Id": self.partner_id,
                    },
                    json={"username": self.api_user, "access_key": self.access_key},
                    timeout=20.0,
                )
            except httpx.TimeoutException:
                logger.warning("siigo_auth_timeout")
                raise HTTPException(status_code=504, detail={
                    "code": "SIIGO_AUTH_TIMEOUT",
                    "message": "Siigo tardó demasiado en responder a la autenticación. Intenta de nuevo.",
                }) from None
            except httpx.RequestError:
                logger.warning("siigo_auth_unavailable")
                raise HTTPException(status_code=502, detail={
                    "code": "SIIGO_AUTH_UNAVAILABLE",
                    "message": "No fue posible conectar con Siigo para autenticar. Intenta de nuevo.",
                }) from None

            if not r.is_success:
                # Do not log the response body: providers can echo credentials or tokens.
                logger.warning(
                    "siigo_auth_rejected upstream_status=%s response_format=%s",
                    r.status_code,
                    "json" if "json" in r.headers.get("content-type", "").lower() else "non_json",
                )

            if r.status_code == 429:
                raise HTTPException(status_code=503, detail={
                    "code": "SIIGO_AUTH_RATE_LIMITED",
                    "message": "Siigo limitó temporalmente la autenticación. Intenta de nuevo más tarde.",
                    "upstream_status": 429,
                })

            if not r.is_success:
                raise HTTPException(status_code=502, detail={
                    "code": "SIIGO_AUTH_FAILED",
                    "message": "Siigo rechazó la autenticación. Revisa las credenciales y permisos de la integración.",
                    "upstream_status": r.status_code,
                })

            try:
                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError("Expected an authentication object")
                token = data.get("access_token") or data.get("token") or data.get("accessToken")
                if not isinstance(token, str) or not token.strip():
                    raise ValueError("Missing authentication token")

                exp = _jwt_exp(token)
                if not exp:
                    expires_in = int(data.get("expires_in") or data.get("expiresIn") or 3600)
                    if expires_in <= 0:
                        raise ValueError("Invalid token lifetime")
                    exp = now + expires_in
            except (ValueError, TypeError, OverflowError):
                logger.warning("siigo_auth_invalid_response upstream_status=%s", r.status_code)
                raise HTTPException(status_code=502, detail={
                    "code": "SIIGO_AUTH_INVALID_RESPONSE",
                    "message": "Siigo devolvió una respuesta de autenticación inválida.",
                }) from None

            self._token = token
            self._token_exp = exp
            return self._token

    def _headers(self) -> Dict[str, str]:
        if not self.partner_id:
            raise HTTPException(status_code=500, detail="Missing SIIGO_PARTNER_ID env var.")
        token = self._get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Partner-Id": self.partner_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def find_client(self, identification: str) -> Optional[Dict[str, Any]]:
        base = self.base_url
        headers = self._headers()

        url1 = f"{base}/customers/{identification}"
        r1 = httpx.get(url1, headers=headers, timeout=20.0)
        if r1.status_code == 200:
            return r1.json()
        if r1.status_code not in (404, 400):
            r1.raise_for_status()

        url2 = f"{base}/customers"
        r2 = httpx.get(url2, headers=headers, params={"identification": identification}, timeout=20.0)
        if r2.status_code == 200:
            data = r2.json()
            if isinstance(data, list):
                return data[0] if data else None
            return data
        if r2.status_code == 404:
            return None

        r2.raise_for_status()
        return None

    def create_client(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        base = self.base_url
        headers = self._headers()
        url = f"{base}/customers"

        r = httpx.post(url, headers=headers, json=payload, timeout=20.0)

        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
         # Devolver el error real de Siigo en vez de 500 genérico
            try:
                body = r.json()
            except Exception:
                body = r.text

            raise HTTPException(
                status_code=r.status_code,
                detail={
                    "message": "Siigo create customer failed",
                    "url": url,
                    "response": body,
                },
            )

        return r.json()


    def create_quote(
        self,
        *,
        customer_identification: str,
        branch_office: int,
        document_id: int,
        seller: int,
        items: List[Dict[str, Any]],
        date_iso: str,
    ) -> Dict[str, Any]:
        base = self.base_url
        url = f"{base}/quotations"

        payload = {
            "document": {"id": int(document_id)},
            "date": date_iso,
            "customer": {"identification": customer_identification, "branch_office": int(branch_office)},
            "seller": int(seller),
            "items": items,
        }

        headers = self._headers()

        for attempt in range(2):
            started = time.monotonic()
            try:
                r = httpx.post(url, headers=headers, json=payload, timeout=quote_timeout())
            except httpx.TimeoutException as exc:
                # A missing response does not prove that Siigo rejected the POST.
                # Do not retry: the quotation may already have been created.
                logger.warning("siigo_quote_timeout error_type=%s", type(exc).__name__)
                raise HTTPException(status_code=504, detail={
                    "code": "SIIGO_QUOTE_TIMEOUT",
                    "message": (
                        "Se agotó el tiempo de espera de la respuesta de Siigo. "
                        "No se pudo confirmar si la cotización fue creada. "
                        "Revisa en Siigo antes de volver a enviarla para evitar duplicados."
                    ),
                    "outcome_unknown": True,
                    "retryable": False,
                }) from None
            except httpx.RequestError as exc:
                logger.warning("siigo_quote_unavailable error_type=%s", type(exc).__name__)
                raise HTTPException(status_code=502, detail={
                    "code": "SIIGO_QUOTE_UNAVAILABLE",
                    "message": (
                        "No fue posible completar la comunicación con Siigo. "
                        "No se pudo confirmar si la cotización fue creada. "
                        "Revisa en Siigo antes de volver a enviarla para evitar duplicados."
                    ),
                    "outcome_unknown": True,
                    "retryable": False,
                }) from None

            logger.info(
                "siigo_quote_response upstream_status=%s duration_ms=%s item_count=%s",
                r.status_code, int((time.monotonic() - started) * 1000), len(items),
            )
            if r.status_code == 429:
                raise HTTPException(status_code=503, detail={
                    "code": "SIIGO_QUOTE_RATE_LIMITED",
                    "message": "Siigo limitó temporalmente los envíos. Intenta de nuevo más tarde.",
                    "upstream_status": 429,
                    "outcome_unknown": False,
                    "retryable": True,
                })

            if r.status_code == 401 and attempt == 0:
                self._invalidate_token()
                headers = self._headers()
                continue

            if r.status_code >= 400:
                try:
                    body = r.json()
                except Exception:
                    body = r.text
                if r.status_code >= 500:
                    is_timeout = r.status_code == 504 or r.headers.get("siigoapi-error-code") == "timed_out"
                    raise HTTPException(status_code=r.status_code, detail={
                        "code": "SIIGO_QUOTE_TIMEOUT" if is_timeout else "SIIGO_QUOTE_UNAVAILABLE",
                        "message": (
                            "Siigo no confirmó la creación de la cotización. "
                            "Usa Verificar en Siigo antes de volver a enviarla para evitar duplicados."
                        ),
                        "upstream_status": r.status_code,
                        "siigoapi_error_code": r.headers.get("siigoapi-error-code"),
                        "outcome_unknown": True, "retryable": False,
                    })
                raise HTTPException(
                    status_code=r.status_code,
                    detail={
                        "message": "Siigo create quotation failed",
                        "url": url,
                        "siigoapi_error_code": r.headers.get("siigoapi-error-code"),
                        "response": body,
                        "outcome_unknown": r.status_code >= 500,
                    },
                )

            try:
                result = r.json()
                if not isinstance(result, dict) or not isinstance(result.get("id"), str) or not result["id"].strip():
                    raise ValueError("Missing quotation identifier")
                if not r.is_success:
                    raise ValueError("Unexpected quotation status")
            except (ValueError, TypeError):
                logger.warning("siigo_quote_invalid_response upstream_status=%s", r.status_code)
                raise HTTPException(status_code=502, detail={
                    "code": "SIIGO_QUOTE_INVALID_RESPONSE",
                    "message": "Siigo devolvió una respuesta sin confirmar la cotización. Revisa en Siigo antes de reenviarla.",
                    "outcome_unknown": True,
                    "retryable": False,
                }) from None
            return result

    def _quote_get(self, path: str, params: dict | None = None) -> dict:
        """Bounded reads; never follow URLs supplied by the provider or caller."""
        headers = self._headers()
        for attempt in range(2):
            try:
                response = httpx.get(f"{self.base_url}/quotations{path}", headers=headers,
                                     params=params, timeout=httpx.Timeout(20, connect=10))
            except httpx.RequestError:
                raise HTTPException(502, detail={
                    "code": "SIIGO_QUOTE_LOOKUP_UNAVAILABLE",
                    "message": "No se pudo consultar Siigo. El envío permanece pendiente de confirmación.",
                    "outcome_unknown": True, "retryable": False,
                }) from None
            if response.status_code == 401 and attempt == 0:
                self._invalidate_token()
                headers = self._headers()
                continue
            if not response.is_success:
                raise HTTPException(502, detail={
                    "code": "SIIGO_QUOTE_LOOKUP_UNAVAILABLE", "upstream_status": response.status_code,
                    "message": "Siigo no permitió verificar la cotización. El envío sigue pendiente.",
                    "outcome_unknown": True, "retryable": False,
                })
            try:
                result = response.json()
                if not isinstance(result, dict):
                    raise ValueError()
                return result
            except (ValueError, TypeError):
                raise HTTPException(502, detail={
                    "code": "SIIGO_QUOTE_LOOKUP_INVALID_RESPONSE",
                    "message": "Siigo devolvió una consulta inválida. El envío sigue pendiente.",
                    "outcome_unknown": True, "retryable": False,
                }) from None

    def list_quotes(self, *, identification: str, branch_office: int,
                    created_start: str, created_end: str, page: int = 1) -> dict:
        result = self._quote_get("", {
            "customer_identification": identification, "customer_branch_office": branch_office,
            "created_start": created_start, "created_end": created_end,
            "page": page, "page_size": 25,
        })
        if not isinstance(result.get("results"), list) or not all(isinstance(q, dict) for q in result["results"]):
            raise HTTPException(502, detail={"code": "SIIGO_QUOTE_LOOKUP_INVALID_RESPONSE",
                "message": "Siigo devolvió un listado inválido. El envío sigue pendiente.",
                "outcome_unknown": True, "retryable": False})
        return result

    def get_quote(self, quotation_id: str) -> dict:
        # Siigo documents UUIDs. Validate before building a path (no query/path injection).
        from uuid import UUID
        try:
            safe_id = str(UUID(quotation_id))
        except (ValueError, TypeError, AttributeError):
            raise HTTPException(422, detail={"code": "INVALID_QUOTATION_ID"}) from None
        result = self._quote_get(f"/{safe_id}")
        if result.get("id") != safe_id:
            raise HTTPException(502, detail={"code": "SIIGO_QUOTE_LOOKUP_INVALID_RESPONSE",
                "message": "Siigo no devolvió el documento solicitado.", "outcome_unknown": True, "retryable": False})
        return result

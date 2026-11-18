# Contexto para próximas sesiones

Este repositorio es el **backend de producción** del cotizador de Siigo.

## Leer primero

- [Operación y diagnóstico de Siigo](docs/siigo-operacion.md): configuración que funcionó, incidente del 2026-11-17, pruebas y despliegue.
- Backend: `Angeloac12/siigo-cotizador`. Frontend de Lovable: `Angeloac12/siigo-quote-buddy`; son repositorios diferentes.
- Autenticación: `SIIGO_SIGNIN_URL=https://api.siigo.com/auth`.
- Operaciones: `SIIGO_BASE_URL=https://api.siigo.com/v1`.
- JSON de autenticación: `username` y `access_key`; incluir `Partner-Id`.
- La URL `services.siigo.com/alliances/api/siigoapi-users/v1/sign-in` devolvió 403 con HTML en el incidente. Revisar este antecedente antes de volver a usarla.
- Autenticación corregida en `902f016`. La actualización posterior de las URLs se hizo en Environment de Render, fuera de Git.
- Refuerzo de creación: `cf634e4`, después del manejo inicial de timeouts en `c07897c`. Espera de lectura por defecto de 120 s; variable opcional `SIIGO_QUOTE_READ_TIMEOUT_SECONDS` (30–300 s).
- Cambio posterior preparado, sin confirmar despliegue: [envíos persistentes y verificación](docs/siigo-envios-pendientes.md). Incluye cola en PostgreSQL, estados y confirmación explícita de documentos existentes. Desplegar backend, función de Supabase y frontend en ese orden.
- Cierre del 2026-11-17: el usuario confirmó que ya funcionó después del último refuerzo. No quedó un identificador de cotización final ni una comprobación independiente de Events/Live. No repetir una creación real solo para volver a verificar este cierre.

## Trabajo y verificación

- Leer el runbook antes de repetir la investigación o pedir datos ya registrados.
- Revisar `git status` y el código actual: este documento registra antecedentes, no sustituye la comprobación del estado vigente.
- Gateway: `app/upstream_gateway/siigo.py`; tiempos: `app/services/siigo_timeouts.py`; reserva persistente: `app/services/quote_commit.py`.
- Regresiones: `tests/test_siigo_auth.py`, `tests/test_siigo_quote.py` y `tests/test_quote_commit_integration.py`. La última validación pasó 110 pruebas, incluidas las de PostgreSQL local aislado y llamadas simuladas a Siigo.
- En un entorno con las dependencias instaladas: `python -m pytest tests -q`.
- Para las pruebas de integración, definir `SIIGO_TEST_DATABASE_URL` con PostgreSQL local aislado; sin esa variable se omiten. Las pruebas crean y eliminan esquemas propios.
- `warnings_json.quote_commit` reserva el borrador antes de enviar. `QUEUED`, `IN_PROGRESS` y `UNKNOWN` impiden reenvíos y edición, incluso tras reiniciar Render. Solo `QUEUED` puede ser consumido por el procesador; no retomar automáticamente `IN_PROGRESS`. No borrar la reserva ni reintentar automáticamente tras un timeout: revisar primero en Siigo y seguir el runbook.
- Cola: `app/services/quote_queue.py`; envío compartido: `app/services/quote_submission.py`; conciliación: `app/services/quote_review.py`. Una consulta vacía nunca libera el borrador. Los intentos antiguos sin payload guardado requieren revisión operativa.
- Un resultado exitoso se guarda como `COMMITTED`/`SUCCEEDED` y se reutiliza en repeticiones. La protección es por borrador; no deduplica borradores distintos.
- La previsualización con `dry_run=true` no verifica la autenticación de Siigo. `/health` tampoco.
- Una prueba de creación con `dry_run=false` genera una cotización real; utilizar los datos autorizados por el usuario.
- En la sesión de este incidente, Render usaba el plan gratuito y no permitía Shell. No proponer una actualización de plan solo para ejecutar la prueba.
- Mantener claves y tokens fuera de documentos, commits y salidas de diagnóstico.
- La optimización de `/match` sigue como mejora independiente; no se abordó en estos arreglos.

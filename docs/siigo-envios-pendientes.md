# Envíos persistentes y verificación de cotizaciones

Implementación preparada el 2026-11-17. Publicación de interfaz y función
confirmada el 2026-11-18 con la evidencia y los límites siguientes.

## Estado verificado del 2026-11-18

**Cierre de publicación documentado.** Interfaz nueva publicada y función
actualizada verificadas. El usuario solicitó cerrar la sesión tras estas
comprobaciones; no se requiere otra creación real para volver a verificarlas.

- Backend: PR #1 integrado en `f46d5bb`. Lovable reportó disponibles los endpoints
  `quote/submission`, `quote/verify` y `quote/confirm`. No se obtuvo una comprobación
  independiente de Events/Live de Render.
- Frontend: PR #1 integrado en `5f968ec`. La web pública
  `https://siigo-quote-quick.lovable.app/` sirvió `/assets/index-BgiU7oNv.js`
  con los controles de verificación, confirmación y recuperación de envíos.
- Función: OPTIONS a
  `https://tuljkmtloqmoksnsmktx.supabase.co/functions/v1/siigo-quote`
  devolvió **200** y `x-fn-version: quote_submission_review_v1_2026-11-17`.
  Fecha de la respuesta: **2026-11-18 18:00:26 UTC**.
- `deeacd4`, comprobado como HEAD del frontend, solo actualizó `deno.lock`.
  La evidencia de publicación de la función es OPTIONS, no ese commit.
- No se comprobó un envío autenticado a Siigo ni se crearon cotizaciones reales.
  No repetir creaciones solo para verificar este cierre. Los intentos antiguos
  con resultado incierto siguen requiriendo revisión operativa.

## Comportamiento

El frontend solicita el envío con `dry_run=false` y `enqueue=true`. El backend
guarda en PostgreSQL el payload exacto y las opciones autorizadas de creación
de cliente antes de responder **202**. La pantalla muestra **En cola** y consulta
el estado cada cinco segundos, sin mantener abierta la petición de creación.
La actualización inicial tarda como máximo unos segundos con el servicio activo.

Un procesador se inicia con el ciclo de vida de FastAPI. Cada proceso ejecuta
un trabajo a la vez, con una pausa de dos segundos entre trabajos. La reserva
atómica por borrador evita dos envíos incluso entre procesos. La cola usa
`warnings_json.quote_commit`; no requiere migración ni un servicio de pago.
`SIIGO_QUOTE_WORKER_ENABLED=false` detiene el consumo de la cola. Su valor por
defecto es `true`. Un servicio detenido no consume trabajos; los `QUEUED` se
recuperan cuando vuelve a arrancar. No se promete ejecución mientras Render
esté suspendido.

| Estado | Pantalla | Permite otro envío |
| --- | --- | --- |
| `NOT_SENT` | Lista para enviar | Sí |
| `QUEUED` | En cola | No |
| `IN_PROGRESS` | Enviando; resultado aún sin confirmar | No |
| `UNKNOWN` | Pendiente de confirmación | No |
| `FAILED` | Envío rechazado; revisar datos | Sí, mediante una nueva acción del usuario |
| `SUCCEEDED` | Cotización creada, con identificador | Reutiliza el resultado guardado |

La pantalla guarda únicamente el identificador del último borrador en el
almacenamiento del navegador, bajo una clave por usuario y organización. Al
volver a la página recupera sus datos del backend. Esta recuperación corresponde
al mismo navegador; no sustituye un historial de todos los envíos. La función de
Supabase comprueba la relación del borrador con un caso de la organización antes
de permitir las nuevas acciones. Un caso sin registrar requiere corregir ese
registro; no se permite saltarse esta comprobación.

## API

Todas las rutas requieren `X-API-Key`, como el resto del backend. La función
de Supabase verifica además la organización del caso.

- `POST /v1/drafts/{id}/quote/commit`: conserva el envío síncrono existente.
  `enqueue=true`, con `dry_run=false`, solicita el envío persistente y devuelve
  `{draft_id, submission}` con 202. Repetir una solicitud encolada devuelve su
  estado sin crear otro trabajo. `dry_run=true` siempre conserva la previsualización.
- `GET /v1/drafts/{id}/quote/submission`: devuelve el estado, mensaje,
  `can_submit`, `can_edit`, `can_verify`, intento y resultado confirmado.
- `GET /v1/drafts/{id}`: añade el mismo objeto `submission` a la respuesta existente.
- `POST /v1/drafts/{id}/quote/verify?page=1`: consulta una página de 25
  cotizaciones en Siigo por cliente, sucursal y fechas. Devuelve candidatos,
  comprobaciones y paginación. No marca el borrador como creado ni lo libera.
- `POST /v1/drafts/{id}/quote/confirm`: recibe `attempt_id`, `quotation_id`
  (UUID de Siigo) y `confirmed=true`. Vuelve a consultar el documento en Siigo,
  comprueba los datos y guarda el resultado con `COMMITTED`/`SUCCEEDED`.

Acciones nuevas en `siigo-quote`: `enqueue`, `submission`, `draft`, `verify`,
`confirm`. Las peticiones de verificación y confirmación solo hacen GET a Siigo.
La confirmación no crea, modifica ni elimina documentos en Siigo.

## Reglas de revisión

La comparación exige cliente, sucursal, comprobante, vendedor, fecha e ítems
completos. Compara códigos, descripciones, cantidades y precios unitarios como
un multiconjunto: conserva líneas repetidas y tolera cambios en el orden.
También exige la fecha real de creación de Siigo dentro del intervalo del envío
(desde cinco minutos antes hasta quince minutos después del inicio). Los impuestos y el total devuelto se muestran
para revisión; no se inventa un total local a partir de precios sin impuestos.

Una coincidencia completa habilita una **confirmación explícita del usuario**;
incluso una única coincidencia nunca se adopta automáticamente. Si hay datos
faltantes o diferencias, requiere revisión operativa. Una búsqueda vacía, una
página incompleta o un fallo de consulta nunca prueban que no se creó el documento
y nunca habilitan reenvíos. El usuario puede consultar otra página o verificar
más tarde.

`IN_PROGRESS` no permite confirmar durante los primeros 15 minutos. Después
permite recuperar explícitamente un proceso interrumpido si se verifica un
documento coincidente. Este plazo **no libera ni reenvía** el borrador. Una
actualización condicional por intento/estado evita sobrescribir una resolución
concurrente o un resultado ya guardado.

Los intentos anteriores a este cambio no guardaron el payload enviado. La API
devuelve `SIIGO_QUOTE_LEGACY_REVIEW_REQUIRED`: seguir la revisión operativa del
[runbook](siigo-operacion.md) sin reconstruir retrospectivamente una certeza.
Esto incluye el timeout aportado del borrador
`5a407411-1c8e-44b9-a2ca-a5b6988e3b38`, terminado a las 21:03:49 UTC con un
504 JSON `timed_out` de Siigo tras 101,648 s. No se concilió ese intento ni se
crearon cotizaciones reales durante esta implementación.

## Despliegue y límites

Este proyecto utiliza el backend administrado por Lovable. La función se
despliega desde el chat de Build de Lovable, conservando los secretos existentes;
publicar la web es una acción aparte. No es necesario que el usuario pueda entrar
al panel de Supabase. El token externo de GitHub Actions devolvía 403 por permisos:
no continuar pidiéndolo ni reintentando esa vía para este proyecto. El workflow
del frontend quedó solo manual y requiere activar explícitamente
`ENABLE_EXTERNAL_SUPABASE_DEPLOY=true`; se mantiene desactivado para Lovable.
La conexión Git de Lovable lee main y permite sincronizar los archivos con
**Re-check**, aunque su chat no pueda abrir el PR privado.

Para publicar cambios exclusivamente documentales de este backend sin reiniciar
el servicio, el mensaje del commit puede incluir `[skip render]`, conforme a la
[documentación de Render](https://render.com/docs/deploys#skipping-an-auto-deploy).
Esto no debe usarse para omitir el despliegue de cambios de ejecución necesarios.

Orden: backend en Render y comprobar Live; después la función `siigo-quote`
en Supabase; finalmente el frontend de Lovable. No activar el frontend nuevo
contra un backend que ignore `enqueue`, pues podría esperar un envío síncrono.
Para revertir la interfaz, resolver o conservar primero todos los trabajos
pendientes. No volver a un backend anterior que desconozca `QUEUED` mientras
existan trabajos en ese estado. No borrar las reservas como parte de un rollback.

La espera a Siigo sigue siendo de 120 s por defecto. Siigo puede devolver su
propio 504 antes de ese plazo; la cola no elimina ese fallo del proveedor.
La idempotencia publicada por Siigo no enumera cotizaciones, por eso no se
reintentan POST con resultado incierto. La protección sigue siendo por borrador;
no deduplica borradores distintos. La optimización de `/match` sigue separada.

Los logs de envío contienen borrador, correlación, intento, duración, número de
ítems y código de resultado. No registran el payload ni cuerpos de autenticación.

## Validación

Ejecutar la suite contra PostgreSQL **local aislado** con `SIIGO_TEST_DATABASE_URL`:

```bash
python -m pytest tests -q
```

Las pruebas crean y eliminan esquemas propios y simulan todas las llamadas a
Siigo. Cubren reinicio, consumidores simultáneos, bloqueo tras timeout, consultas
vacías o inválidas, confirmación explícita, diferencias en el documento y
reutilización del resultado. El frontend incorpora pruebas de recuperación,
estado en cola y confirmación de candidatos, además de build y TypeScript.

Fuentes oficiales consultadas:

- [Listar cotizaciones](https://developers.siigo.com/docs/siigoapi/cotizaciones/5-get-quotations)
- [Consultar una cotización](https://developers.siigo.com/docs/siigoapi/cotizaciones/4-get-quotation)
- [Manejo de errores](https://developers.siigo.com/docs/siigoapi/manejo-de-errores/)
- [Alcance de la idempotencia](https://developers.siigo.com/docs/siigoapi/idempotencia)

Validación local del cambio: 110 pruebas de backend con PostgreSQL aislado y 18
pruebas de frontend; build de producción, TypeScript y Deno correctos. La
comprobación en navegador con respuestas simuladas cubrió escritorio (1280 px),
móvil (390 px), consulta vacía, confirmación explícita y recuperación al recargar,
sin errores de JavaScript ni desbordamiento horizontal. No se probaron envíos reales.

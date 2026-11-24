# Operación del cotizador y diagnóstico de Siigo

Última actualización: 2026-11-24.

Corrección del 2026-11-23: el NIT bloqueado después de un envío era un fallo del
frontend, ya publicado y confirmado por el usuario. Detalle en [formulario
bloqueado tras un envío](siigo-envios-pendientes.md#formulario-bloqueado-tras-un-envío-2026-11-23).

Implementación posterior: [envíos persistentes, estados y verificación
de cotizaciones](siigo-envios-pendientes.md). El 2026-11-18 se confirmaron la
interfaz publicada y la versión nueva de la función mediante OPTIONS. El backend
está integrado en main y Lovable reportó los endpoints disponibles; no se
comprobó Events/Live de Render de forma independiente. No se crearon cotizaciones
reales ni se conciliaron los intentos anteriores.

## Referencia rápida

**Estado al cierre del 2026-11-17: el usuario confirmó que ya funcionó**, después
de publicar el refuerzo
[`cf634e4`](https://github.com/Angeloac12/siigo-cotizador/commit/cf634e4a023c15dac3621a514cc98f55c93c46a3)
y solicitar su despliegue en Render. Dar el incidente por resuelto según esa
confirmación. No volver a repetir envíos reales solo para verificar el cierre.
No se aportaron el identificador de la cotización final, un log final de éxito
ni una captura de Events/Live; la confirmación es del usuario, no una
verificación independiente del agente.

La autenticación operativa usa `https://api.siigo.com/auth`, con JSON
`{"username": "<usuario API>", "access_key": "<clave API>"}` y encabezado
`Partner-Id`. Las operaciones usan `https://api.siigo.com/v1`.

El 2026-11-17 se corrigió el gateway en el commit
[`902f016`](https://github.com/Angeloac12/siigo-cotizador/commit/902f016d9a64233f9ddb0d260f701a4b36817785).
Después se actualizaron las URLs en Render. Los fallos posteriores de timeout
se corrigieron en dos pasos:

| Commit | Cambio |
| --- | --- |
| `902f016` | Autenticación corregida y errores de autenticación controlados |
| `c07897c` | Timeout de creación controlado como 504, con resultado sin confirmar |
| `cf634e4` | Espera de lectura de 120 s, llamadas en hilo de trabajo y reserva persistente por borrador |

El estado de funcionamiento confirmado corresponde al último refuerzo; el 504
de los logs intermedios no confirmaba por sí solo una creación exitosa.

## Dónde está cada parte

| Parte | Referencia |
| --- | --- |
| Backend local | `/Users/angeloandrescarohernandez/siigo-cotizador` |
| Backend en GitHub | https://github.com/Angeloac12/siigo-cotizador |
| Servicio de Render | https://dashboard.render.com/web/srv-d5rnsa24d50c7396m560 |
| Frontend en GitHub | https://github.com/Angeloac12/siigo-quote-buddy |
| Proyecto de Lovable | https://lovable.dev/projects/7163e2ba-02d5-4f97-bdf5-f91e8d69d030 |

Flujo: frontend de Lovable → función de Supabase `siigo-quote` → backend de
Render → Siigo. La URL pública del frontend no quedó registrada en esta sesión.

## Configuración en Render

Servicio → **Environment** → **Edit**.

| Variable | Valor o significado |
| --- | --- |
| `SIIGO_ENABLED` | `true` |
| `SIIGO_SIGNIN_URL` | `https://api.siigo.com/auth` |
| `SIIGO_BASE_URL` | `https://api.siigo.com/v1` |
| `SIIGO_API_USER` | Usuario de las credenciales API de Siigo; valor privado |
| `SIIGO_ACCESS_KEY` | Clave API de Siigo; valor privado |
| `SIIGO_PARTNER_ID` | Identificador real de la integración, de 3 a 100 caracteres alfanuméricos |
| `SIIGO_QUOTE_READ_TIMEOUT_SECONDS` | Opcional: espera de lectura al crear cotizaciones; por defecto `120` segundos. Admite de 30 a 300; un valor inválido usa 120. |

Guardar y desplegar con la opción disponible en Render. Esperar a **Live** en
**Events**. Un cambio de variables no cambia el commit de GitHub. El gateway
conserva configuración y token en memoria; aplicar el cambio mediante despliegue
o reinicio del proceso.

## Incidente del 2026-11-17

1. La carga, extracción y edición de ítems funcionaban. `/match` tardó unos 72 s;
   esa lentitud es independiente del error de autenticación y quedó sin optimizar.
2. El primer `/quote/commit` devolvía 200 porque era una previsualización
   (`dry_run=true`), no un envío a Siigo.
3. El envío real fallaba en `_get_token()`: la URL configurada de
   `services.siigo.com/.../sign-in` devolvía 403. `httpx.HTTPStatusError` escapaba
   del endpoint, que solo capturaba `HTTPException`, y producía un 500.
4. El commit `902f016` cambió `userName` por `username`, añadió `Partner-Id` a
   autenticación y convirtió rechazos, errores de red y respuestas inválidas en
   errores estructurados. También añadió pruebas de regresión.
5. Tras desplegar ese commit, producción seguía mostrando:

   ```text
   siigo_auth_rejected upstream_status=403 response_format=non_json
   SIIGO_AUTH_FAILED
   ```

   El endpoint devolvía ahora 502 controlado. Eso confirmó el despliegue del
   arreglo, pero todavía no resolvía el acceso a Siigo.
6. Se compararon desde el entorno local ambas URLs mediante POST con JSON vacío,
   sin credenciales: la URL de `services.siigo.com` devolvió 403 y HTML; `/auth`
   en `api.siigo.com` devolvió 400 y JSON con `parameter_required` para los dos
   campos. Esta comparación comprobó el comportamiento de las rutas desde ese
   entorno; no fue una prueba de credenciales desde Render.
7. Se indicó cambiar las URLs en Environment de Render por las de la tabla
   anterior. El usuario confirmó el cambio y posteriormente que quedó hecho.

Una imprecisión corregida durante la investigación: el SDK recibe parámetros
JavaScript `userName` y `accessKey`, pero el JSON que envía realmente contiene
`username` y `access_key`. No copiar los nombres de los argumentos del SDK como
si fueran los del cuerpo HTTP.

## Incidente posterior: timeout al crear la cotización

Los logs aportados después muestran un fallo distinto del rechazo de
autenticación: `httpx.ReadTimeout` dentro de `create_quote()`, durante el POST a
`/quotations` con `timeout=30.0`. La preparación de encabezados con token ya
había terminado; el traceback no confirma que Siigo haya aceptado el documento.

El envío real del borrador `1b2bc4df-5ff8-4ad6-9f3e-37f0cc795e90`, después de
editar los ítems a las 16:52:39 UTC del 2026-11-17, terminó en 500 porque ese
error de red no se capturaba. Los commits de 162 y 180 ms, dentro del flujo
`/v1/quote-drafts/process`, no muestran el cuerpo de la petición ni el resultado
de Siigo y no demuestran creación real. `/match` tardó 13,867 y 47,180 s en
las dos ejecuciones mostradas.

Primera corrección publicada en `c07897c`: los timeouts de creación
devuelven 504 con `SIIGO_QUOTE_TIMEOUT`; otros errores de transporte devuelven
502 con `SIIGO_QUOTE_UNAVAILABLE`. Ambos incluyen `outcome_unknown=true` y
`retryable=false`: son una indicación conservadora de que se debe comprobar el
resultado en Siigo antes de repetir el envío. No se reintenta automáticamente
un POST tras un error de transporte. Esto controla el error; no resuelve la
demora del proveedor ni bloquea un nuevo envío manual desde el frontend.
El log aportado de las 17:11:12 UTC confirma que este manejo ya estaba activo:
devolvió `SIIGO_QUOTE_TIMEOUT` y 504 después de 31,841 s.

Refuerzo posterior publicado en `cf634e4`, tras ese log, y funcionamiento
confirmado por el usuario al cierre:

- La espera de lectura pasa de 30 a **120 segundos** por defecto, de acuerdo
  con la [recomendación oficial de Siigo](https://developers.siigo.com/docs/siigoapi/manejo-de-errores/).
  La conexión mantiene un límite de 10 s; la escritura, de 30 s. El proxy
  interno deja margen adicional para autenticación y creación de cliente.
- La llamada síncrona a Siigo se ejecuta en un hilo de trabajo: no bloquea el
  bucle de eventos de FastAPI durante toda la espera.
- Antes de enviar, se reserva el borrador mediante una actualización atómica
  en PostgreSQL. La reserva se guarda en `warnings_json.quote_commit`; no
  requiere migraciones ni cambios de variables en Render.
- `IN_PROGRESS` bloquea otro envío mientras se procesa el primero. `UNKNOWN`
  bloquea el reenvío y la edición cuando no se pudo confirmar el resultado.
  La protección persiste entre procesos y reinicios. No caduca automáticamente.
- Un rechazo confirmado deja `FAILED` y permite corregir y volver a enviar;
  un resultado válido con identificador se guarda como `SUCCEEDED` y
  `COMMITTED`. Las repeticiones posteriores recuperan el resultado guardado.
- Una respuesta inválida o un fallo al guardar el resultado después del POST
  no libera el borrador para un nuevo envío.

Si aparece `SIIGO_QUOTE_REVIEW_REQUIRED` o una reserva queda en `IN_PROGRESS`
tras un reinicio, revisar en Siigo y resolver mediante una conexión operativa
autorizada a la base. Si existe el documento, guardar la respuesta verificada
en `siigo_quote_response` y marcar `COMMITTED`/`SUCCEEDED`. Solo si se confirma
que no se creó, cambiar la reserva a `FAILED`, conservando la evidencia de la
revisión. El frontend no puede saltarse el bloqueo modificando los ítems.
No hay liberación automática: la ausencia de respuesta no confirma ausencia
del documento.

Los timeouts anteriores a este refuerzo no tienen reserva persistida; en
particular, revisar el borrador `1b2bc4df-5ff8-4ad6-9f3e-37f0cc795e90` antes
de repetirlo. No se han conciliado ni modificado los datos de producción.
La protección es por borrador; no deduplica borradores distintos con el mismo
contenido. Una caída de Siigo puede seguir superando los 120 s.

La [documentación de idempotencia de Siigo](https://developers.siigo.com/docs/siigoapi/idempotencia)
enumera facturas, notas crédito, comprobantes contables y recibos de caja;
no confirma ese soporte para cotizaciones. Por eso no se usan reintentos de
POST tras un resultado incierto basados únicamente en ese encabezado.

Antes de repetir la cotización fallida, revisar en Siigo las cotizaciones del
cliente y la fecha correspondiente. Un timeout puede ocurrir después de que
Siigo haya creado el documento. Si existe, conservar su número o identificador;
si no se puede determinar el resultado, mantenerlo como pendiente de verificación.
No atribuir los apagados del proceso a este timeout: el fragmento contiene
apagados ordenados, pero no su causa.

## Verificar producción sin Shell

En este incidente, el plan gratuito de Render no incluía Shell. Usar la app:

1. Confirmar el despliegue **Live** y las URLs de Environment.
2. Revisar si el intento previo ya creó un documento en Siigo. Si el borrador
   está en `UNKNOWN` o quedó en `IN_PROGRESS` tras un reinicio, resolver la
   revisión operativa descrita arriba antes de otro envío.
3. Solo para una nueva prueba real autorizada, revisar cliente, productos y cantidades.
4. Pulsar **Crear cotización en Siigo** una vez; este paso crea el documento real.
5. Comprobar el número o identificador devuelto y el registro real en Siigo,
   además del estado HTTP del envío.
6. Si falla, conservar hora, `correlation_id`, `draft_id`, código de error y
   `upstream_status`. Revisar primero el diagnóstico siguiente.

Este procedimiento queda para incidencias o verificaciones futuras. Al cierre
no se ejecutó otra creación: el usuario ya había confirmado el funcionamiento.

La lentitud variable de `/match` queda como mejora separada, no como un fallo
pendiente de autenticación o creación. No se concilió desde esta sesión la
posible existencia de documentos de los intentos anteriores con timeout.

Ni `/health` ni un commit en modo prueba prueban la autenticación. El endpoint
de consulta de clientes actualmente captura cualquier excepción y devuelve
`exists=false`: esa respuesta tampoco demuestra que la autenticación funcionó.

## Interpretación de errores

| Resultado | Qué revisar |
| --- | --- |
| `SIIGO_AUTH_FAILED`, 403 y `non_json` | URL efectiva de autenticación y posible rechazo de infraestructura; no atribuirlo automáticamente a las claves |
| `SIIGO_AUTH_FAILED` con respuesta JSON | Credenciales y permisos API; inspeccionar el diagnóstico de forma privada |
| `SIIGO_AUTH_RATE_LIMITED` | Límite de autenticación; evitar reintentos continuos |
| `SIIGO_AUTH_TIMEOUT` / `SIIGO_AUTH_UNAVAILABLE` | Conectividad con Siigo |
| `SIIGO_AUTH_INVALID_RESPONSE` | Respuesta sin token válido, JSON inválido o expiración inválida |
| `SIIGO_QUOTE_TIMEOUT` / `SIIGO_QUOTE_UNAVAILABLE` | Resultado de creación sin confirmar; comprobar en Siigo antes de repetir el envío |
| `SIIGO_QUOTE_IN_PROGRESS` / `SIIGO_QUOTE_REVIEW_REQUIRED` | El borrador ya tiene un envío en curso o sin confirmar; no repetirlo ni crear otro para eludir el bloqueo |
| `SIIGO_QUOTE_INVALID_RESPONSE` | Siigo no devolvió un identificador válido; revisar el documento antes de repetir |
| 200 en previsualización | Payload preparado; todavía no se autenticó ni creó una cotización |

El gateway registra estado y formato de respuesta, sin cuerpo de autenticación,
credenciales ni token. El endpoint de commit añade la correlación al diagnóstico.

## Pruebas locales y fuentes

Con Python 3.13 o posterior y un entorno virtual, instalar las dependencias de
`requirements.txt` y las de pruebas (`pytest`, `httpx==0.28.1`, `reportlab`). Ejecutar:

```bash
python -m pytest tests/test_siigo_auth.py -q
python -m pytest tests/test_siigo_quote.py -q
python -m pytest tests -q
```

Validación del arreglo: **44 pruebas pasaron**, de ellas 22 de autenticación,
en un entorno temporal con Python 3.14.7, FastAPI 0.124.4, Starlette 0.50.0 y
Pydantic 2.12.5. Todas las llamadas a Siigo de esas pruebas fueron simuladas.

Validación adicional del manejo de errores de creación: **52 pruebas pasaron**,
incluidas 8 nuevas pruebas de transporte, ausencia de reintentos tras timeout,
renovación de token y respuesta exitosa. No se crearon cotizaciones reales.

Las pruebas de persistencia y concurrencia usan PostgreSQL y llamadas simuladas
a Siigo. Ejecutarlas contra una instancia local aislada con
`SIIGO_TEST_DATABASE_URL` definido; cada prueba crea y elimina su propio esquema.
Si no se define esa variable, esas pruebas se omiten.

Validación del refuerzo: **79 pruebas pasaron**, incluidas 10 con PostgreSQL
real aislado. Se verificaron dos reservas simultáneas, respuesta de `/health`
durante una llamada lenta, bloqueo tras timeout o reinicio, fallo de persistencia
tras una creación y timeout en el segundo envío después de crear el cliente.
Todas las llamadas a Siigo fueron simuladas.

- [Autenticación oficial](https://developers.siigo.com/docs/siigoapi/autenticacion/autenticacion/)
- [Partner-Id](https://developers.siigo.com/docs/siigoapi/partner-id/)
- [JSON de autenticación del SDK oficial](https://github.com/SiigoSAS/siigo_sdk_javascript/blob/dev/src/ApiClient.js#L659)

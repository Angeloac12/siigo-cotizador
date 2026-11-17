# Operación del cotizador y diagnóstico de Siigo

Última actualización: 2026-11-17.

## Referencia rápida

La autenticación operativa usa `https://api.siigo.com/auth`, con JSON
`{"username": "<usuario API>", "access_key": "<clave API>"}` y encabezado
`Partner-Id`. Las operaciones usan `https://api.siigo.com/v1`.

El 2026-11-17 se corrigió el gateway en el commit
[`902f016`](https://github.com/Angeloac12/siigo-cotizador/commit/902f016d9a64233f9ddb0d260f701a4b36817785).
Después se actualizaron las URLs en Render. El usuario reportó que quedó hecho
tras la prueba en producción. No se adjuntó a esta sesión el identificador de la
cotización final ni un log final de éxito; no asumir una verificación independiente.

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

Corrección local preparada (pendiente de despliegue): los timeouts de creación
devuelven 504 con `SIIGO_QUOTE_TIMEOUT`; otros errores de transporte devuelven
502 con `SIIGO_QUOTE_UNAVAILABLE`. Ambos incluyen `outcome_unknown=true` y
`retryable=false`: son una indicación conservadora de que se debe comprobar el
resultado en Siigo antes de repetir el envío. No se reintenta automáticamente
un POST tras un error de transporte. Esto controla el error; no resuelve la
demora del proveedor ni bloquea un nuevo envío manual desde el frontend.

Antes de repetir la cotización fallida, revisar en Siigo las cotizaciones del
cliente y la fecha correspondiente. Un timeout puede ocurrir después de que
Siigo haya creado el documento. Si existe, conservar su número o identificador;
si no se puede determinar el resultado, mantenerlo como pendiente de verificación.
No atribuir los apagados del proceso a este timeout: el fragmento contiene
apagados ordenados, pero no su causa.

## Verificar producción sin Shell

En este incidente, el plan gratuito de Render no incluía Shell. Usar la app:

1. Confirmar el despliegue **Live** y las URLs de Environment.
2. Retomar la cotización autorizada que falló y revisar cliente, productos y cantidades.
3. Pulsar **Crear cotización en Siigo** una vez; este paso crea el documento real.
4. Comprobar el número o identificador devuelto y el registro real en Siigo,
   además del estado HTTP del envío.
5. Si falla, conservar hora, `correlation_id`, `draft_id`, código de error y
   `upstream_status`. Revisar primero el diagnóstico siguiente.

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

- [Autenticación oficial](https://developers.siigo.com/docs/siigoapi/autenticacion/autenticacion/)
- [Partner-Id](https://developers.siigo.com/docs/siigoapi/partner-id/)
- [JSON de autenticación del SDK oficial](https://github.com/SiigoSAS/siigo_sdk_javascript/blob/dev/src/ApiClient.js#L659)

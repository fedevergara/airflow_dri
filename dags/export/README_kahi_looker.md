# UdeA Scientific Production Looker Export

DAG creado:
- `export_udea_scientific_production_looker`

Este DAG carga la producción científica de la UdeA en Google Sheets para visualizarla en Looker Studio. No lee MongoDB desde el worker de Airflow; ejecuta por SSH un script en el servidor remoto donde MongoDB puede responder por `localhost`.

## 1. Archivos remotos

En el servidor remoto crea esta estructura:

```bash
export KAHI_EXPORT_DIR=/ruta/remota/kahi_exports

mkdir -p "$KAHI_EXPORT_DIR/secrets"
python3 -m venv "$KAHI_EXPORT_DIR/venv"
"$KAHI_EXPORT_DIR/venv/bin/pip" install pandas pymongo google-api-python-client google-auth google-auth-oauthlib
```

También puedes regenerar/validar todo con el bootstrap remoto:

```bash
cd "$KAHI_EXPORT_DIR"
python3 setup_kahi_export.py
```

Ese script crea directorios, `.env`, `requirements.txt`, venv y dependencias. Si necesitas migrar un token existente, pásalo explícitamente con `--legacy-token-path /ruta/al/token.pickle`.

Copia el script versionado en este repo:

```bash
scp config/scripts/export_kahi_looker.py USUARIO@SERVIDOR:"$KAHI_EXPORT_DIR/export_kahi_looker.py"
scp config/scripts/setup_kahi_export_remote.py USUARIO@SERVIDOR:"$KAHI_EXPORT_DIR/setup_kahi_export.py"
ssh USUARIO@SERVIDOR "chmod 750 '$KAHI_EXPORT_DIR/export_kahi_looker.py' '$KAHI_EXPORT_DIR/setup_kahi_export.py'"
```

El token OAuth de Google Sheets debe vivir solo en el servidor remoto:

```bash
$KAHI_EXPORT_DIR/secrets/token.pickle
chmod 600 "$KAHI_EXPORT_DIR/secrets/token.pickle"
```

Si MongoDB necesita una URI distinta a `mongodb://localhost:27017/`, guárdala en un `.env` remoto y no en Airflow:

```bash
cat > "$KAHI_EXPORT_DIR/.env" <<'EOF'
MONGO_URI='mongodb://localhost:27017/'
KAHI_DB=kahi
KAHI_LOOKER_SPREADSHEET_ID='SPREADSHEET_ID'
EOF
chmod 600 "$KAHI_EXPORT_DIR/.env"
```

## 2. Connection de Airflow

Crea una connection SSH:

- `ssh_kahi_default`: servidor remoto donde corre MongoDB/Kahi.

Si prefieres reutilizar una connection existente, define la Variable `KAHI_LOOKER_SSH_CONN_ID`.

## 3. Variables de Airflow

Variables principales:

- `KAHI_LOOKER_SSH_CONN_ID`: default `ssh_kahi_default`.
- `KAHI_LOOKER_REMOTE_DIR`: directorio remoto de la app, por ejemplo `/ruta/remota/kahi_exports`.
- `KAHI_LOOKER_REMOTE_PYTHON`: por ejemplo `/ruta/remota/kahi_exports/venv/bin/python`.
- `KAHI_LOOKER_REMOTE_SCRIPT`: por ejemplo `/ruta/remota/kahi_exports/export_kahi_looker.py`.
- `KAHI_LOOKER_REMOTE_SETUP_SCRIPT`: por ejemplo `/ruta/remota/kahi_exports/setup_kahi_export.py`.
- `KAHI_LOOKER_REMOTE_ENV`: por ejemplo `/ruta/remota/kahi_exports/.env`.
- `KAHI_LOOKER_TOKEN_PATH`: por ejemplo `/ruta/remota/kahi_exports/secrets/token.pickle`.
- `KAHI_LOOKER_DB`: default `kahi`.
- `KAHI_LOOKER_CMD_TIMEOUT`: default `7200`.
- `KAHI_LOOKER_CHUNK_SIZE`: default `5000`.
- `KAHI_LOOKER_MIN_WRITE_INTERVAL_SECONDS`: default `1.2`; separación mínima entre escrituras para respetar la cuota de Google Sheets.
- `KAHI_LOOKER_MAX_API_ATTEMPTS`: default `8`; reintentos para respuestas 429 y errores transitorios 5xx.
- `KAHI_LOOKER_DRY_RUN`: default `false`; usa `true` para probar el DAG sin escribir en Google Sheets.

## 4. Prueba remota manual

Antes de activar el DAG:

```bash
ssh USUARIO@SERVIDOR
cd /ruta/remota/kahi_exports
./venv/bin/python export_kahi_looker.py \
  --institution-id 03bp5hc83 \
  --db kahi \
  --mongo-uri mongodb://localhost:27017/ \
  --token-path /ruta/remota/kahi_exports/secrets/token.pickle \
  --dry-run
```

Luego prueba sin `--dry-run`.

## 5. Seguridad operacional

- El token de Google no se guarda en el repo ni en Airflow.
- Airflow solo ejecuta por SSH y registra métricas.
- El DAG usa `max_active_runs=1`.
- El comando remoto usa `flock` para evitar dos exportaciones simultáneas.
- El script crea backup del spreadsheet antes de limpiar y escribir datos.
- Antes de escribir, el script comprueba que las columnas sobrantes no tengan valores ni fórmulas. Si encuentra contenido, detiene el export sin eliminarlas.
- Las cuadrículas se ajustan al tamaño exacto de cada salida, incluidas las
  pestañas optimizadas para el dashboard.
- La limpieza se limita a las columnas reales de cada pestaña; no se usan rangos amplios como `A:ZZ`.
- Las escrituras se limitan a menos de 60 solicitudes por minuto y usan espera exponencial para recuperarse de errores de cuota 429 y fallos transitorios 5xx.

## 6. Modelo optimizado para Looker Studio

Las pestañas `works`, `subjects` y `affiliations` se conservan para auditoría y
detalle. El dashboard no debe mezclarlas entre sí. Debe conectarse a estas cinco
pestañas puente, generadas en la misma lectura de MongoDB:

| Pestaña | Granularidad | Uso en el dashboard |
|---|---|---|
| `dashboard_general` | un producto | métricas generales y filtros por año/colaboración |
| `dashboard_paises` | producto–país extranjero | KPI internacional, número de países, mapa, cronología y principales países |
| `dashboard_instituciones` | producto–institución externa, nacional o internacional | principales organizaciones |
| `dashboard_temas` | producto–tema OpenAlex de nivel 0 | distribución temática |
| `dashboard_grupos` | producto–grupo/unidad UdeA | principales grupos y filtros académicos |

Volúmenes del `dry-run` del 25 de agosto de 2026:

- `dashboard_general`: 33.737 filas.
- `dashboard_paises`: 35.224 filas.
- `dashboard_instituciones`: 123.882 filas; 68.605 nacionales, 55.162
  internacionales y 115 sin país informado.
- `dashboard_temas`: 70.053 filas.
- `dashboard_grupos`: 85.293 filas.

### Reconstrucción del dashboard de referencia

Configuración recomendada de cada elemento:

| Elemento | Fuente | Dimensión | Métrica / filtro |
|---|---|---|---|
| Productos internacionales | `dashboard_paises` | — | `COUNT_DISTINCT(colav_id)` |
| Cantidad de países | `dashboard_paises` | — | `COUNT_DISTINCT(país)` |
| Mapa | `dashboard_paises` | `país` | `COUNT_DISTINCT(colav_id)` |
| Cronología | `dashboard_paises` | `año` | `COUNT_DISTINCT(colav_id)` |
| Principales países | `dashboard_paises` | `país` | `COUNT_DISTINCT(colav_id)` |
| Principales organizaciones | `dashboard_instituciones` | `institución` | `COUNT_DISTINCT(colav_id)`; puede segmentarse por `colaboración` |
| Productos por tema | `dashboard_temas` | `categoría_tema` | `SUM(asignaciones)` y filtro `colaboración = Internacional` cuando se quiera el alcance internacional |
| Principales grupos | `dashboard_grupos` | `grupo` | `COUNT_DISTINCT(colav_id)` y filtro `grupo` no vacío |

Las categorías temáticas del PDF se preservan como `Medicine`, `Biology`,
`Chemistry`, `Physics`, `Computer science` y `Otros`. Solo se exportan temas
OpenAlex de nivel 0 para evitar que conceptos de distintos niveles inflen la
gráfica.

Filtros sin blends:

- `año`: disponible en las cinco fuentes.
- `país`: usar en `dashboard_paises` y `dashboard_instituciones`.
- `tema`: usar en `dashboard_temas`.
- `unidad_académica` y `grupo`: usar en `dashboard_grupos`.
- `colaboración`: disponible en `dashboard_general`, `dashboard_instituciones`,
  `dashboard_temas` y `dashboard_grupos`.

`dashboard_instituciones` incluye instituciones colombianas y extranjeras, pero
excluye la Universidad de Antioquia y sus dependencias. En esta pestaña,
`colaboración` clasifica cada institución como `Nacional`, `Internacional` o
`Sin país`.

Para que el reporte responda rápido, cada gráfica debe usar directamente su
pestaña puente y no un blend entre fuentes. Las pestañas detalladas pueden
quedar en una página secundaria de consulta.

Las filas de `dashboard_grupos` con `grupo` vacío se conservan porque todavía
pueden aportar facultad, departamento o unidad académica. Deben excluirse solo
de la gráfica de principales grupos, no de la fuente completa.

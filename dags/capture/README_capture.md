# Capture DAGs (Google Sheets -> MongoDB via SSH tunnel)

DAGs creados:
- `capture_mobilities`
- `capture_agreements`

## 1. Dependencias de providers
En `.env` define:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-google apache-airflow-providers-mongo apache-airflow-providers-ssh openpyxl
```

Luego reinicia:

```bash
docker compose down
docker compose up -d
```

## 2. Connections requeridas
Crea estas conexiones en Airflow UI (`Admin -> Connections`):

- `ssh_capture_default` (tipo SSH): host, puerto 22, usuario y llave/clave del servidor donde vive MongoDB.
- `mongo_default` (tipo MongoDB): credenciales de Mongo (usuario/password y DB). El host/puerto no se exponen porque el DAG conecta por túnel.

Para Google Sheets tienes 2 opciones:
- Opción A (service account): `google_sheets_default` (tipo Google Cloud) + variable `GOOGLE_SHEETS_CONN_ID` (opcional).
- Opción B: `GOOGLE_SHEETS_TOKEN_PICKLE_B64` o `GOOGLE_SHEETS_TOKEN_PICKLE_PATH`.
  - Si existe alguna de esas variables, el DAG usa `token.pickle` (con refresh) y no requiere `google_sheets_default`.

Si quieres otros IDs, define Variables:
- `GOOGLE_SHEETS_CONN_ID`
- `MONGO_CAPTURE_CONN_ID`
- `MONGO_CAPTURE_DB` (default: `international`)
- `MONGO_CAPTURE_SSH_CONN_ID` (default: `ssh_capture_default`)
- `MONGO_CAPTURE_REMOTE_HOST` (default: `127.0.0.1`)
- `MONGO_CAPTURE_REMOTE_PORT` (default: `27017`)

## 3. Variables requeridas por fuente
### Mobilities (international + national)
- `GSHEET_MOBILITIES_INTERNATIONAL_ID`
- `GSHEET_MOBILITIES_NATIONAL_ID`
- `SHEET_ENTRANTE` (ejemplo: `Movilidad Entrante`)
- `SHEET_SALIENTE` (ejemplo: `Movilidad Saliente`)

### Agreements (international + national)
- `GSHEET_AGREEMENTS_INTERNATIONAL_ID` (Drive file id del `.xlsx`)
- `GSHEET_AGREEMENTS_INTERNATIONAL_RANGE` (opcional, hojas separadas por coma; default `ACTIVOS,INACTIVOS`)
- `GSHEET_AGREEMENTS_NATIONAL_ID` (Drive file id del `.xlsx`)
- `GSHEET_AGREEMENTS_NATIONAL_RANGE` (opcional, hojas separadas por coma; default `ACTIVOS,INACTIVOS`)
- `GOOGLE_DRIVE_TOKEN_PICKLE_PATH` (opcional, default `/opt/airflow/config/secrets/agreements/agreements_token.pickle`)

## 4. Colecciones Mongo destino (raw)
- Base de datos: `international`
- Colección para `capture_mobilities`: `mobilities`
- Colección para `capture_agreements`: `agreements`

Cada documento se guarda con metadata de captura (`source`, `extracted_at`, `airflow_run_id`, `row_number`, `row_fingerprint`) y `raw_payload`.
Además, para movilidad, se agrega:
- `vía`: `Entrante` / `Saliente`
- `ambito`: `internacional` / `nacional`

## 5. `raw_row` y duplicidad
- `raw_row` es la fila original como lista ordenada de celdas (sin nombres de columna), útil para trazabilidad y debugging.
- `raw_payload` es la misma fila pero en formato clave/valor usando los headers del Sheet.
- Si defines solo el nombre de hoja (ej. `Movilidad Entrante`), el DAG expande a `A:ZZZ` para cubrir columnas más allá de `Z`.
- Para evitar duplicados en re-ejecuciones del DAG, la carga usa `upsert` por (`source`, `row_fingerprint`):
  - Si la fila ya existe, se actualiza metadata (`last_*`) y no crea un nuevo documento.
  - Si no existe, se inserta como nuevo documento (`first_*`).

## 6. Seguridad
- No guardes passwords, llaves privadas ni URIs en el repo.
- Usa solo Airflow `Connections`/`Variables` para secretos.

## 7. OAuth Drive sin login manual cada corrida
Para fuentes de convenios que viven como archivo en Google Drive (por ejemplo `.xlsx`), usa:

`/opt/airflow/config/scripts/bootstrap_drive_token.py`

Flujo:
- Primera vez: ejecutas el script con `--client-secrets` para consentir OAuth una sola vez.
- Siguientes corridas: el token se refresca automáticamente (sin login manual).

Ejemplo:

```bash
docker compose exec airflow-worker python /opt/airflow/config/scripts/bootstrap_drive_token.py \
  --token-path /opt/airflow/config/secrets/agreements/agreements_token.pickle \
  --client-secrets /opt/airflow/config/secrets/agreements/credentials.json \
  --file-id 1iXADw9QAsWiZYXeFrf6W8uoiO20ydFY8
```

Si no puedes usar callback local, agrega `--use-console`.

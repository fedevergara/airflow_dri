# Capture DAGs (Google Sheets -> MongoDB via SSH tunnel)

DAGs creados:
- `capture_mobilities`
- `capture_agreements`

## 1. Dependencias de providers
En `.env` define:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-google apache-airflow-providers-mongo apache-airflow-providers-ssh
```

Luego reinicia:

```bash
docker compose down
docker compose up -d
```

## 2. Connections requeridas
Crea estas conexiones en Airflow UI (`Admin -> Connections`):

- `google_sheets_default` (tipo Google Cloud): credenciales de Service Account con acceso a tus Sheets.
- `ssh_capture_default` (tipo SSH): host, puerto 22, usuario y llave/clave del servidor donde vive MongoDB.
- `mongo_default` (tipo MongoDB): credenciales de Mongo (usuario/password y DB). El host/puerto no se exponen porque el DAG conecta por túnel.

Si quieres otros IDs, define Variables:
- `GOOGLE_SHEETS_CONN_ID`
- `MONGO_CAPTURE_CONN_ID`
- `MONGO_CAPTURE_DB` (default: `capture_raw`)
- `MONGO_CAPTURE_SSH_CONN_ID` (default: `ssh_capture_default`)
- `MONGO_CAPTURE_REMOTE_HOST` (default: `127.0.0.1`)
- `MONGO_CAPTURE_REMOTE_PORT` (default: `27017`)

## 3. Variables requeridas por fuente
### Mobilities
- `GSHEET_MOBILITIES_INTERNATIONAL_ID`
- `GSHEET_MOBILITIES_NATIONAL_ID`
- `GSHEET_MOBILITIES_INTERNATIONAL_RANGE` (opcional, default `A:Z`)
- `GSHEET_MOBILITIES_NATIONAL_RANGE` (opcional, default `A:Z`)

### Agreements
- `GSHEET_AGREEMENTS_INTERNATIONAL_ID`
- `GSHEET_AGREEMENTS_NATIONAL_ID`
- `GSHEET_AGREEMENTS_INTERNATIONAL_RANGE` (opcional, default `A:Z`)
- `GSHEET_AGREEMENTS_NATIONAL_RANGE` (opcional, default `A:Z`)

## 4. Colecciones Mongo destino (raw)
- `mobilities_international_raw`
- `mobilities_national_raw`
- `agreements_international_raw`
- `agreements_national_raw`

Cada documento se guarda con metadata de captura (`source`, `extracted_at`, `airflow_run_id`, `row_number`, `row_fingerprint`) y `raw_payload`.

## 5. Seguridad
- No guardes passwords, llaves privadas ni URIs en el repo.
- Usa solo Airflow `Connections`/`Variables` para secretos.

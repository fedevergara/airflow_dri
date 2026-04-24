# UdeA Scientific Production Looker Load

DAG creado:
- `load_udea_scientific_production_looker`

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

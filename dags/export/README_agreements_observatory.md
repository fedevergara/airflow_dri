# Agreements Export

DAG creado:
- `export_agreements_observatory`

Este DAG toma los convenios crudos guardados en MongoDB por `capture_agreements` y exporta el resultado a un Google Sheet configurado en el `.env` remoto.

Sigue el patrón de movilidades: Airflow no procesa MongoDB directamente desde el worker, sino que ejecuta por SSH un script Python en el servidor remoto donde MongoDB responde por `localhost`.

## 1. Archivos remotos

En el servidor remoto crea esta estructura:

```bash
export AGREEMENTS_EXPORT_DIR=/ruta/remota/agreements_export

python3 -m venv "$AGREEMENTS_EXPORT_DIR/venv"
"$AGREEMENTS_EXPORT_DIR/venv/bin/pip" install pandas pymongo google-api-python-client google-auth google-auth-oauthlib
```

También puedes bootstrapear todo con:

```bash
cd "$AGREEMENTS_EXPORT_DIR"
python3 setup_agreements_export.py
```

Copia los scripts versionados:

```bash
scp config/scripts/export_agreements_observatory.py USUARIO@SERVIDOR:"$AGREEMENTS_EXPORT_DIR/export_agreements_observatory.py"
scp config/scripts/setup_agreements_export_remote.py USUARIO@SERVIDOR:"$AGREEMENTS_EXPORT_DIR/setup_agreements_export.py"
ssh USUARIO@SERVIDOR "chmod 750 '$AGREEMENTS_EXPORT_DIR/export_agreements_observatory.py' '$AGREEMENTS_EXPORT_DIR/setup_agreements_export.py'"
```

El export reutiliza el mismo token OAuth de Google Sheets usado por movilidades:

```bash
$AGREEMENTS_EXPORT_TOKEN_PATH
chmod 600 "$AGREEMENTS_EXPORT_TOKEN_PATH"
```

Configura MongoDB y el destino en un `.env` remoto. El valor real de `AGREEMENTS_EXPORT_SPREADSHEET_ID` no debe quedar documentado en el repositorio.

```bash
cat > "$AGREEMENTS_EXPORT_DIR/.env" <<'EOF'
MONGO_URI='mongodb://localhost:27017/'
AGREEMENTS_RAW_DB='international'
AGREEMENTS_RAW_COLLECTION='agreements'
AGREEMENTS_RAW_SOURCE_TYPE='agreements'
AGREEMENTS_EXPORT_SPREADSHEET_ID='SPREADSHEET_ID'
AGREEMENTS_EXPORT_SHEET_NAME='convenios'
EOF
chmod 600 "$AGREEMENTS_EXPORT_DIR/.env"
```

## 2. Connection de Airflow

Usa una connection SSH al servidor remoto. Por defecto el DAG reutiliza:

- `ssh_capture_default`

Si prefieres otra, define `AGREEMENTS_EXPORT_SSH_CONN_ID`.

## 3. Variables de Airflow

- `AGREEMENTS_EXPORT_SSH_CONN_ID`: default `ssh_capture_default`
- `AGREEMENTS_EXPORT_REMOTE_DIR`: ruta remota configurada en Airflow.
- `AGREEMENTS_EXPORT_REMOTE_PYTHON`: ruta remota al Python del venv configurada en Airflow.
- `AGREEMENTS_EXPORT_REMOTE_SCRIPT`: ruta remota al script configurada en Airflow.
- `AGREEMENTS_EXPORT_REMOTE_SETUP_SCRIPT`: ruta remota al script de setup configurada en Airflow.
- `AGREEMENTS_EXPORT_REMOTE_ENV`: ruta remota al `.env` configurada en Airflow.
- `AGREEMENTS_EXPORT_TOKEN_PATH`: ruta remota al token configurada en Airflow.
- `AGREEMENTS_EXPORT_DB`: default `international`
- `AGREEMENTS_EXPORT_COLLECTION`: default `agreements`
- `AGREEMENTS_EXPORT_SOURCE_TYPE`: default `agreements`
- `AGREEMENTS_EXPORT_SHEET_NAME`: default `convenios`
- `AGREEMENTS_EXPORT_CMD_TIMEOUT`: default `7200`
- `AGREEMENTS_EXPORT_BATCH_SIZE`: default `1000`
- `AGREEMENTS_EXPORT_CHUNK_SIZE`: default `5000`
- `AGREEMENTS_EXPORT_DRY_RUN`: default `false`
- `AGREEMENTS_EXPORT_SKIP_BACKUP`: default `false`

## 4. Salida

La hoja resultante queda normalizada con columnas coherentes con el export de movilidades:

- `ámbito`
- `país_local`
- `institución_local`
- `continente`
- `código`
- `país/ciudad`
- `institución/entidad`
- `fecha_de_inicio`
- `fecha_de_finalización`
- `fecha_de_inicio_unix`
- `fecha_de_finalización_unix`
- `modalidad`
- `categorías`
- `resúmen`
- `idioma`
- `facultades`
- `unidad_académica_administrativa`
- `enlace`
- `estado`

`estado` sale de la columna original cuando existe, con fallback a la hoja original del archivo (`ACTIVOS` o `INACTIVOS`). La salida se normaliza solo como `vigente` o `vencido`.

## 5. Prueba remota manual

```bash
ssh USUARIO@SERVIDOR
cd /ruta/remota/agreements_export
./venv/bin/python export_agreements_observatory.py \
  --db international \
  --collection agreements \
  --sheet-name convenios \
  --token-path /ruta/remota/secrets/token.pickle \
  --dry-run
```

Luego prueba sin `--dry-run`.

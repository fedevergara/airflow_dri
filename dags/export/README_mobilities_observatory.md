# Mobilities Export

DAG creado:
- `export_mobilities_observatory`

Este DAG toma las movilidades crudas guardadas en MongoDB, las normaliza y exporta el resultado a un Google Sheet. Sigue el mismo patrón del DAG de producción científica: Airflow no procesa MongoDB directamente desde el worker, sino que ejecuta por SSH un script Python en el servidor remoto.

## 1. Archivos remotos

En el servidor remoto crea esta estructura:

```bash
export MOBILITIES_TRANSFORM_DIR=/ruta/remota/mobilities_transform

mkdir -p "$MOBILITIES_TRANSFORM_DIR/secrets"
python3 -m venv "$MOBILITIES_TRANSFORM_DIR/venv"
"$MOBILITIES_TRANSFORM_DIR/venv/bin/pip" install pandas pymongo google-api-python-client google-auth google-auth-oauthlib
```

También puedes bootstrapear todo con:

```bash
cd "$MOBILITIES_TRANSFORM_DIR"
python3 setup_mobilities_transform.py
```

Copia los scripts versionados:

```bash
scp config/scripts/transform_mobilities_observatory.py USUARIO@SERVIDOR:"$MOBILITIES_TRANSFORM_DIR/transform_mobilities_observatory.py"
scp config/scripts/setup_mobilities_transform_remote.py USUARIO@SERVIDOR:"$MOBILITIES_TRANSFORM_DIR/setup_mobilities_transform.py"
ssh USUARIO@SERVIDOR "chmod 750 '$MOBILITIES_TRANSFORM_DIR/transform_mobilities_observatory.py' '$MOBILITIES_TRANSFORM_DIR/setup_mobilities_transform.py'"
```

El token OAuth de Google Sheets debe vivir solo en el servidor remoto:

```bash
$MOBILITIES_TRANSFORM_DIR/secrets/token.pickle
chmod 600 "$MOBILITIES_TRANSFORM_DIR/secrets/token.pickle"
```

Si MongoDB necesita una URI distinta a `mongodb://localhost:27017/`, guárdala en un `.env` remoto:

```bash
cat > "$MOBILITIES_TRANSFORM_DIR/.env" <<'EOF'
MONGO_URI='mongodb://localhost:27017/'
MOBILITIES_RAW_DB='international'
MOBILITIES_RAW_COLLECTION='mobilities'
MOBILITIES_RAW_SOURCE_TYPE='mobilities'
MOBILITIES_TRANSFORM_SPREADSHEET_ID='1vhZIZ2To_PSe8PldkfLr56hoboMZ6HpMmDWtfV7Wh2k'
MOBILITIES_TRANSFORM_SHEET_NAME='mobilidades'
EOF
chmod 600 "$MOBILITIES_TRANSFORM_DIR/.env"
```

## 2. Connection de Airflow

Usa una connection SSH al servidor remoto. Por defecto el DAG reutiliza:

- `ssh_capture_default`

Si prefieres otra, define `MOBILITIES_TRANSFORM_SSH_CONN_ID`.

## 3. Variables de Airflow

Variables principales:

Por compatibilidad, se mantienen con prefijo `MOBILITIES_TRANSFORM_` para no obligarte a reconfigurar todo en este paso.

- `MOBILITIES_TRANSFORM_SSH_CONN_ID`: default `ssh_capture_default`
- `MOBILITIES_TRANSFORM_REMOTE_DIR`: default `/srv/mobilities_transform`
- `MOBILITIES_TRANSFORM_REMOTE_PYTHON`: default `/srv/mobilities_transform/venv/bin/python`
- `MOBILITIES_TRANSFORM_REMOTE_SCRIPT`: default `/srv/mobilities_transform/transform_mobilities_observatory.py`
- `MOBILITIES_TRANSFORM_REMOTE_SETUP_SCRIPT`: default `/srv/mobilities_transform/setup_mobilities_transform.py`
- `MOBILITIES_TRANSFORM_REMOTE_ENV`: default `/srv/mobilities_transform/.env`
- `MOBILITIES_TRANSFORM_TOKEN_PATH`: default `/srv/mobilities_transform/secrets/token.pickle`
- `MOBILITIES_TRANSFORM_DB`: default `international`
- `MOBILITIES_TRANSFORM_COLLECTION`: default `mobilities`
- `MOBILITIES_TRANSFORM_SOURCE_TYPE`: default `mobilities`
- `MOBILITIES_TRANSFORM_SPREADSHEET_ID`: default `1vhZIZ2To_PSe8PldkfLr56hoboMZ6HpMmDWtfV7Wh2k`
- `MOBILITIES_TRANSFORM_SHEET_NAME`: default `mobilidades`
- `MOBILITIES_TRANSFORM_CMD_TIMEOUT`: default `7200`
- `MOBILITIES_TRANSFORM_BATCH_SIZE`: default `1000`
- `MOBILITIES_TRANSFORM_CHUNK_SIZE`: default `5000`
- `MOBILITIES_TRANSFORM_DRY_RUN`: default `false`
- `MOBILITIES_TRANSFORM_SKIP_BACKUP`: default `false`

## 4. Salida

La hoja resultante se escribe en una sola pestaña y deja todas las columnas como string:

- `año`
- `semestre`
- `modalidad`
- `ambito`
- `vía`
- `categoría`
- `país/ciudad`
- `unidad_académica_administrativa`
- `programa_académico`
- `objetivo_de_la_movilidad`
- `categoría_estudiantes`
- `institución/entidad`
- `movilidad_por_convenio`
- `código_convenio`
- `campus`
- `country/city`

## 5. Prueba remota manual

```bash
ssh USUARIO@SERVIDOR
cd /ruta/remota/mobilities_transform
./venv/bin/python transform_mobilities_observatory.py \
  --db international \
  --collection mobilities \
  --spreadsheet-id 1vhZIZ2To_PSe8PldkfLr56hoboMZ6HpMmDWtfV7Wh2k \
  --sheet-name movilidades \
  --token-path /ruta/remota/mobilities_transform/secrets/token.pickle \
  --dry-run
```

Luego prueba sin `--dry-run`.

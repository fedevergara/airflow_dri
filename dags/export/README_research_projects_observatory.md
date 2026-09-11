# Research Projects Observatory Export

El DAG `export_research_projects_observatory` lee proyectos desde un Google
Sheets de origen, selecciona participaciones de entidades distintas de la
Universidad de Antioquia y reemplaza de forma controlada la pestaña destino.

## Documentos

Los identificadores de los documentos no se versionan. Se configuran mediante
las variables de Airflow `RESEARCH_PROJECTS_SOURCE_SPREADSHEET_ID` y
`RESEARCH_PROJECTS_TARGET_SPREADSHEET_ID`. La pestaña predeterminada en ambos
documentos es `Hoja 1`.

El origen es de solo lectura. Antes de modificar el destino, el script crea
una copia completa del documento, salvo que se use explícitamente
`--skip-backup`.

## Selección y granularidad

- Se omiten las filas sin `RAZON_SOCIAL_APORTANTE` utilizable.
- Se omiten las filas donde la entidad es `UDEA` o `Universidad de Antioquia`.
- Por lo tanto, los proyectos que solo tienen participación de la UdeA no
  aparecen en el destino.
- Se omiten las filas que no tienen `AÑO_INICIO`, `FECHA_INICIO` ni
  `FECHA_FINALIZACION`.
- Los nombres de instituciones y entidades se normalizan completamente en
  mayúsculas, incluidos artículos y conectores como `OF`, `FOR`, `THE`, `DE`,
  `DEL` y `Y`.
- Se eliminan repeticiones producidas por el cruce de participantes,
  aportantes y grupos. El código del proyecto forma parte de la salida y de la
  llave de deduplicación.

Con el origen revisado el 19 de agosto de 2026, la transformación encuentra
2.717 proyectos, selecciona 1.126 con entidades distintas de la UdeA y datos
temporales, y genera 4.169 filas deduplicadas.

## Mapeo

| Origen | Destino |
|---|---|
| `CODIGO_PROYECTO` | `código_proyecto` |
| `AÑO_INICIO` | `año` |
| `FECHA_INICIO` | `fecha_de_inicio` |
| `FECHA_FINALIZACION` | `fecha_de_finalización` |
| `Area OCDE, con respectoa los grupos` | `area` |
| `GRUPO` | `grupo` |
| `RAZON_SOCIAL_APORTANTE` | `institución/entidad` |
| `TIPO_APORTANTE` | `tipo_aportante` |
| `TOTAL_FRESCO` | `fresco` |
| `TOTAL_ESPECIE` | `especie` |
| `MONEDA` | `moneda` |
| `PAIS_ENTIDAD` | `pais` |

Las fechas se publican como `DD/MM/YYYY`. Los valores fresco y especie se
convierten a números y la moneda se publica en mayúsculas.

## Operación

El DAG corre los viernes a las 16:00, hora de Bogotá, y usa una sola tarea SSH
que ejecuta la extracción, transformación y carga. El horario evita competir
por cuota con el exportador de producción científica de las 15:00.

Variables de Airflow:

- `RESEARCH_PROJECTS_EXPORT_SSH_CONN_ID`: default `ssh_kahi_default`.
- `RESEARCH_PROJECTS_EXPORT_REMOTE_DIR`: directorio remoto del script.
- `RESEARCH_PROJECTS_EXPORT_REMOTE_PYTHON`: intérprete remoto.
- `RESEARCH_PROJECTS_EXPORT_REMOTE_SCRIPT`: ruta remota del script.
- `RESEARCH_PROJECTS_EXPORT_REMOTE_ENV`: `.env` remoto opcional.
- `RESEARCH_PROJECTS_EXPORT_TOKEN_PATH`: token OAuth de Google.
- `RESEARCH_PROJECTS_SOURCE_SPREADSHEET_ID`: documento de origen.
- `RESEARCH_PROJECTS_TARGET_SPREADSHEET_ID`: documento destino.
- `RESEARCH_PROJECTS_SOURCE_SHEET_NAME`: default `Hoja 1`.
- `RESEARCH_PROJECTS_TARGET_SHEET_NAME`: default `Hoja 1`.
- `RESEARCH_PROJECTS_EXPORT_CHUNK_SIZE`: default `1000`.
- `RESEARCH_PROJECTS_EXPORT_MIN_WRITE_INTERVAL_SECONDS`: default `1.2`.
- `RESEARCH_PROJECTS_EXPORT_MAX_API_ATTEMPTS`: default `8`.
- `RESEARCH_PROJECTS_EXPORT_CMD_TIMEOUT`: default `1800`.
- `RESEARCH_PROJECTS_EXPORT_DRY_RUN`: default `false`.
- `RESEARCH_PROJECTS_EXPORT_SKIP_BACKUP`: default `false`.

`DEPENDENCIA_PARTICIPANTE` no se captura y no se crea una columna de unidad
académica o administrativa en el destino.

La cuadrícula destino se ajusta exactamente a 12 columnas y al número de filas
generado. Antes de eliminar columnas sobrantes se comprueba, incluyendo
fórmulas, que estén vacías. Las escrituras se limitan para respetar la cuota de
Google Sheets y los errores 429/5xx se reintentan con espera exponencial.

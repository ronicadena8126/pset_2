# PSet 2: Pipeline de datos end-to-end para anticipar la mora en tarjetas de crédito

Pipeline ELT reproducible para el proyecto del PSet 1, que predice con un mes de anticipación qué clientes de tarjeta de crédito no pagarán.

```
UCI (.xls) → Kestra → RAW → dbt → BRONZE → dbt → SILVER → dbt → GOLD (estrella) → Spark → OBT
                                     Snowflake (base PSET2_MORA)
```

| Capa | Esquema | Quién la construye | Contenido |
|---|---|---|---|
| Raw | `RAW` | Kestra (`uci_ingest_monthly`) | `CLIENTS_SNAPSHOT`, `STATEMENTS`, `DEFAULT_LABELS`, con los valores originales y metadatos de carga |
| Bronze | `BRONZE` | dbt (vistas) | Copia 1:1 tipada y con nombres estandarizados |
| Silver | `SILVER` | dbt (tablas) | Datos limpios: deduplicación, recodificación y banderas de calidad |
| Gold | `GOLD` | dbt (tablas) | Esquema estrella: `fct_monthly_statement`, `fct_default_outcome`, `dim_customer`, `dim_month`, `dim_payment_status` |
| OBT | `OBT` | Spark (`spark/build_obt.py`) | `OBT_CUSTOMER_DEFAULT`, con una fila por cliente y su target |

## Estructura

```
pset_2/
├── docker-compose.yml        Kestra + Postgres, Spark master/worker, dbt (perfil "tools")
├── Dockerfile                imagen de la ingesta (pset2-ingesta:1.0) que corre Kestra
├── requirements.txt          librerías de esa imagen
├── .env.example              variables necesarias (copiar a .env)
├── kestra/
│   ├── flows/                uci_ingest_monthly, uci_backfill, pipeline_transform
│   └── scripts/ingest_uci.py extracción + carga idempotente a RAW
├── dbt/                      proyecto dbt (bronze/silver/gold + tests)
├── spark/build_obt.py        construcción y validación de la OBT
├── scripts/                  encode_secrets.sh, deploy_kestra.sh
└── docs/                     memo, diagramas, snowflake_setup.sql
```

## Requisitos

- El Codespace del curso (o Docker Desktop, Java 17 y Python 3.11). **Usa una máquina de 4 núcleos**, porque Kestra y Spark corren al mismo tiempo. Las UIs se abren desde la pestaña **Ports** de VS Code.
- Una cuenta de Snowflake (sirve la de prueba gratuita)
- `curl` y `bash`

Relación con las capas vistas en clase: Bronze equivale a la copia tipada de RAW, Silver a *staging* (la limpieza) y Gold a los *marts* (el esquema estrella).

## 1. Preparar Snowflake

Ejecuta `docs/snowflake_setup.sql` en un worksheet de Snowflake. Crea el warehouse `PSET2_WH`, la base `PSET2_MORA`, los esquemas `RAW`, `BRONZE`, `SILVER`, `GOLD` y `OBT`, y el rol y el usuario de servicio.

## 2. Configurar credenciales

```bash
cp .env.example .env            # completar con tus valores
./scripts/encode_secrets.sh     # genera .env_encoded (SECRET_* en base64 para Kestra)
```

Ni `.env` ni `.env_encoded` se versionan (están en `.gitignore`). Los flujos leen las credenciales con `{{ secret('SNOWFLAKE_...') }}`.

## 3. Construir la imagen de ingesta y levantar la infraestructura

```bash
docker build -t pset2-ingesta:1.0 .     # la imagen que corre Kestra (pullPolicy: NEVER)
docker compose up -d                    # postgres, kestra (v2.0.7), spark-master, spark-worker
docker compose ps
```

Si cambias el `Dockerfile` o `ingest_uci.py`, reconstruye la imagen con un tag nuevo y actualiza `containerImage` en `kestra/flows/uci_ingest_monthly.yml`.

- UI de Kestra: http://localhost:8080 (por defecto `admin@kestra.io` / `Admin1234`, como en el lab de la Semana 4)
- UI de Spark: http://localhost:8082

## 4. Publicar flujos y archivos en Kestra

```bash
bash scripts/deploy_kestra.sh
```

Sigue el mismo patrón que `subir_a_kestra.sh` del laboratorio: sube el proyecto dbt y el job de Spark como *namespace files* de `usfq.pset2`, y crea o actualiza los tres flujos.

## 5. Ejecutar el pipeline

### Ingesta: backfill histórico (primera vez)

En la UI de Kestra: **Flows → usfq.pset2 → uci_backfill → Execute**. Por defecto carga abr–sep 2005, un mes por vez.

O por API:

```bash
curl -u admin@kestra.io:Admin1234 -X POST \
  http://localhost:8080/api/v1/main/executions/usfq.pset2/uci_backfill
```

Para recargar un solo mes, ejecuta `uci_ingest_monthly` con el input `statement_month=2005-07`. La carga es idempotente: borra e inserta la partición dentro de una transacción.

### Ingesta programada

`uci_ingest_monthly` corre sola el día 2 de cada mes a las 06:00 (America/Guayaquil) y carga el mes anterior. Tiene 4 retries exponenciales (30 s a 10 min) y un bloque `errors`. Como la fuente es estática, una corrida programada fuera de abr–sep 2005 termina en estado `NO_PARTITION`, sin cargar nada.

### Transformación (dbt + Spark)

`pipeline_transform` se dispara automáticamente cuando `uci_ingest_monthly` termina en SUCCESS. Ejecuta `dbt build` (modelos + tests) y después el job de Spark. Si algún test falla, Spark no corre.

### Ejecución manual de cada herramienta

```bash
# dbt (modelos + tests) contra Snowflake
docker compose run --rm dbt build --target prod
docker compose run --rm dbt docs generate --target prod

# Spark sobre el cluster local, leyendo GOLD y escribiendo OBT en Snowflake
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1 \
  /opt/spark-apps/build_obt.py --mode snowflake
```

El job imprime las validaciones (`[validation] ...`) y solo escribe la OBT si todas pasan.

## Prueba local sin Snowflake (opcional)

Todo el pipeline se puede probar con DuckDB como destino, con las librerías que ya trae el Codespace del curso:

```bash
pip install xlrd   # el resto (pandas, duckdb, dbt-duckdb, pyspark) ya viene en requirements.txt del curso
for m in 2005-04 2005-05 2005-06 2005-07 2005-08 2005-09; do
  python kestra/scripts/ingest_uci.py --statement-month $m --target duckdb --duckdb-path local.duckdb
done
DUCKDB_PATH=$PWD/local.duckdb dbt build --project-dir dbt --profiles-dir dbt --target local
# exportar GOLD a parquet y correr Spark en modo local
python -c "import duckdb;c=duckdb.connect('local.duckdb');[c.execute(f\"COPY GOLD.{t} TO 'spark/gold_parquet/{t}.parquet'\") for t in ['fct_monthly_statement','fct_default_outcome','dim_customer','dim_month','dim_payment_status']]"
python spark/build_obt.py --mode local --local-dir spark/gold_parquet
```

Resultado de referencia: RAW con 180.000 estados de cuenta y 30.000 etiquetas; `dbt build` con 72/72 OK; OBT con 30.000 filas × 53 columnas.

## Fuente de datos

Yeh, I. C. & Lien, C. H. (2009). *Default of Credit Card Clients*. UCI Machine Learning Repository, licencia CC BY 4.0. URL configurable en `UCI_SOURCE_URL`.

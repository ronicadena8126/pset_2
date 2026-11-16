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
├── Dockerfile.transform      imagen de dbt + Spark (pset2-transform:1.0)
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

- Docker Desktop (o el Codespace del curso con una máquina de **4 núcleos**). Las UIs se abren desde la pestaña **Ports**.
- Python **3.11** para correr dbt/Spark fuera de Docker (Python 3.12+ no es compatible con pyspark 3.5). Con `uv`: `uv venv -p 3.11 .venv`.
- Una cuenta de Snowflake (sirve la de prueba gratuita) y `openssl`, `curl`, `bash`.

Relación con las capas vistas en clase: Bronze equivale a la copia tipada de RAW, Silver a *staging* (la limpieza) y Gold a los *marts* (el esquema estrella).

## 1. Preparar Snowflake (autenticación key-pair)

Snowflake no admite usuarios de servicio que entren solo con contraseña, así que el pipeline usa key-pair:

```bash
mkdir -p ~/.snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/pset2_rsa_key.p8 -nocrypt
chmod 600 ~/.snowflake/pset2_rsa_key.p8
openssl rsa -in ~/.snowflake/pset2_rsa_key.p8 -pubout 2>/dev/null | grep -v 'PUBLIC KEY' | tr -d '\n'   # llave pública
```

Abre `docs/snowflake_setup.sql`, reemplaza `<PEGA_AQUI_TU_LLAVE_PUBLICA>` por la llave pública y ejecútalo completo (**Run All**) como `ACCOUNTADMIN`. Crea el warehouse, la base `PSET2_MORA`, los esquemas `RAW`/`BRONZE`/`SILVER`/`GOLD`/`OBT`, el rol `PSET2_ROLE` y el usuario de servicio `PSET2_SVC`.

## 2. Configurar credenciales

```bash
cp .env.example .env                 # completa SNOWFLAKE_ACCOUNT (orgname-accountname) y la ruta ABSOLUTA de la llave
bash scripts/encode_secrets.sh       # genera .env_encoded: SECRET_* en base64 para Kestra, incluida la llave privada
```

Ni `.env` ni `.env_encoded` ni la llave se versionan (`.gitignore`). Los flujos leen todo con `{{ secret('SNOWFLAKE_...') }}`, y cada contenedor escribe la llave en `/tmp/sf_key.p8`.

## 3. Construir las imágenes y levantar Kestra

```bash
docker build -t pset2-ingesta:1.0 .                                  # ingesta (Kestra la corre con pullPolicy: NEVER)
docker build -f Dockerfile.transform -t pset2-transform:1.0 .        # dbt + Spark (Java 17)
docker compose up -d postgres kestra                                 # Kestra v2.0.7 + Postgres
```

UI de Kestra: http://localhost:8080 (`admin@kestra.io` / `Admin1234`, como en el lab de la Semana 4). Si cambias `.env_encoded`, recrea Kestra con `docker compose up -d --force-recreate kestra`.

Spark corre dentro de `pset2-transform` en modo local. `spark-master` y `spark-worker` del compose son opcionales; levántalos con `docker compose up -d spark-master spark-worker` si quieres usar el clúster.

## 4. Publicar flujos y archivos en Kestra

```bash
bash scripts/deploy_kestra.sh
```

Sube el proyecto dbt, el job de Spark y el script de ingesta como *namespace files* de `usfq.pset2`, y crea o actualiza los tres flujos.

## 5. Ejecutar el pipeline

| Orden | Flujo | Qué hace | Resultado esperado |
|---|---|---|---|
| 1 | `uci_backfill` | Carga abr–sep 2005 a RAW, mes a mes e idempotente | SUCCESS; 180.000 filas en `RAW.STATEMENTS` |
| 2 | `pipeline_transform` | `dbt build` (Bronze → Silver → Gold + tests) y después Spark (OBT) | SUCCESS; `PASS=72`; 30.000 filas en `OBT.OBT_CUSTOMER_DEFAULT` |

En la UI: **Flows → usfq.pset2 → (flujo) → Execute**.

**Programación:** `uci_ingest_monthly` corre el día 2 de cada mes a las 06:00 (America/Guayaquil) y carga el mes anterior, con 4 retries exponenciales (30 s a 10 min). `pipeline_transform` corre el mismo día a las 07:00. Si un test de dbt falla, Spark no corre. Como la fuente es estática, una corrida programada fuera de abr–sep 2005 termina en `NO_PARTITION` sin cargar nada.

### Ejecución sin Kestra (opcional, desde tu máquina)

```bash
uv venv -p 3.11 .venv && source .venv/bin/activate
uv pip install pandas==2.2.2 pyarrow==16.1.0 xlrd requests "snowflake-connector-python[pandas]" dbt-snowflake pyspark==3.5.3
set -a; source .env; set +a
for m in 2005-04 2005-05 2005-06 2005-07 2005-08 2005-09; do
  python kestra/scripts/ingest_uci.py --statement-month $m --target snowflake --local-file default_of_credit_card_clients.xls
done
dbt build --project-dir dbt --profiles-dir dbt --target prod
# En Mac con VPN o proxy, fuerza que Spark use localhost:
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS="--conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 --conf spark.ui.enabled=false pyspark-shell"
python spark/build_obt.py --mode snowflake-py
```

El archivo fuente se descarga con:
`curl -L -o default_of_credit_card_clients.xls https://raw.githubusercontent.com/jphall663/interpretable_machine_learning_with_python/master/default_of_credit_card_clients.xls`

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

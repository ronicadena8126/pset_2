"""
Ingesta de la fuente UCI "Default of Credit Card Clients" hacia el esquema RAW.

La fuente es un único archivo .xls con 30.000 clientes y 6 meses de historia
(abril-septiembre 2005) en formato ancho. Para simular el corte mensual que
entregaría el core de tarjetas, cada ejecución carga UNA partición mensual:

    statement_month  columnas de la fuente
    2005-09          PAY_0, BILL_AMT1, PAY_AMT1
    2005-08          PAY_2, BILL_AMT2, PAY_AMT2
    ...
    2005-04          PAY_6, BILL_AMT6, PAY_AMT6

Los valores NO se transforman (sin limpieza, sin recodificación): solo se
selecciona la columna del mes y se agregan metadatos de carga.

Tablas destino (esquema RAW):
  RAW.CLIENTS_SNAPSHOT   atributos del cliente, una copia por mes cargado
  RAW.STATEMENTS         estado de cuenta mensual (cliente x mes)
  RAW.DEFAULT_LABELS     target; solo se observa después del corte 2005-09

La carga es idempotente: borra la partición del mes y la vuelve a insertar,
de modo que un retry o un backfill repetido no duplica filas.

Uso:
  python ingest_uci.py --statement-month 2005-09 --target snowflake
  python ingest_uci.py --statement-month 2005-09 --target duckdb --duckdb-path local.duckdb
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger("ingest_uci")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_SOURCE_URL = (
    "https://archive.ics.uci.edu/static/public/350/default+of+credit+card+clients.zip"
)
# Mapeo mes de estado de cuenta -> sufijos de columnas en la fuente
MONTH_COLUMNS = {
    "2005-09": ("PAY_0", "BILL_AMT1", "PAY_AMT1"),
    "2005-08": ("PAY_2", "BILL_AMT2", "PAY_AMT2"),
    "2005-07": ("PAY_3", "BILL_AMT3", "PAY_AMT3"),
    "2005-06": ("PAY_4", "BILL_AMT4", "PAY_AMT4"),
    "2005-05": ("PAY_5", "BILL_AMT5", "PAY_AMT5"),
    "2005-04": ("PAY_6", "BILL_AMT6", "PAY_AMT6"),
}
LABEL_MONTH_TRIGGER = "2005-09"  # el target de oct-2005 se conoce al cerrar sep-2005
LABEL_MONTH = "2005-10"
CLIENT_COLUMNS = ["ID", "LIMIT_BAL", "SEX", "EDUCATION", "MARRIAGE", "AGE"]
TARGET_COLUMN = "default payment next month"
EXPECTED_ROWS = 30_000


class SourceSchemaError(Exception):
    """La fuente cambió de estructura: no tiene sentido reintentar."""


# --------------------------------------------------------------------------- extract
def read_source(source_url: str | None, local_file: str | None) -> pd.DataFrame:
    if local_file:
        log.info("Leyendo fuente local %s", local_file)
        raw_bytes = open(local_file, "rb").read()
    else:
        import requests

        log.info("Descargando fuente %s", source_url)
        resp = requests.get(source_url, timeout=120)
        resp.raise_for_status()  # error HTTP -> excepción -> Kestra reintenta
        raw_bytes = resp.content

    if raw_bytes[:2] == b"PK":  # zip publicado por UCI
        import zipfile

        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            name = next(n for n in zf.namelist() if n.endswith(".xls"))
            raw_bytes = zf.read(name)

    # La primera fila del .xls es un encabezado X1..X23/Y; la segunda tiene los nombres reales
    df = pd.read_excel(io.BytesIO(raw_bytes), header=1)
    expected = set(CLIENT_COLUMNS) | {c for cols in MONTH_COLUMNS.values() for c in cols} | {TARGET_COLUMN}
    missing = expected - set(df.columns)
    if missing:
        raise SourceSchemaError(f"Columnas faltantes en la fuente: {sorted(missing)}")
    if len(df) != EXPECTED_ROWS:
        log.warning("La fuente tiene %s filas (esperadas %s)", len(df), EXPECTED_ROWS)
    if df["ID"].duplicated().any():
        raise SourceSchemaError("La fuente trae IDs duplicados")
    return df


def build_partitions(df: pd.DataFrame, month: str, meta: dict) -> dict[str, pd.DataFrame]:
    pay, bill, amt = MONTH_COLUMNS[month]
    clients = df[CLIENT_COLUMNS].copy()
    clients["SNAPSHOT_MONTH"] = month

    stm = pd.DataFrame(
        {
            "ID": df["ID"],
            "PAY_STATUS": df[pay],
            "BILL_AMT": df[bill],
            "PAY_AMT": df[amt],
            "SOURCE_COLUMNS": f"{pay}|{bill}|{amt}",
            "STATEMENT_MONTH": month,
        }
    )
    out = {"CLIENTS_SNAPSHOT": clients, "STATEMENTS": stm}
    if month == LABEL_MONTH_TRIGGER:
        out["DEFAULT_LABELS"] = pd.DataFrame(
            {
                "ID": df["ID"],
                "DEFAULT_PAYMENT_NEXT_MONTH": df[TARGET_COLUMN],
                "LABEL_MONTH": LABEL_MONTH,
            }
        )
    for t in out.values():
        for k, v in meta.items():
            t[k] = v
    return out


PARTITION_COL = {
    "CLIENTS_SNAPSHOT": "SNAPSHOT_MONTH",
    "STATEMENTS": "STATEMENT_MONTH",
    "DEFAULT_LABELS": "LABEL_MONTH",
}

DDL = {
    "CLIENTS_SNAPSHOT": """
        ID NUMBER, LIMIT_BAL NUMBER, SEX NUMBER, EDUCATION NUMBER, MARRIAGE NUMBER, AGE NUMBER,
        SNAPSHOT_MONTH VARCHAR, _INGESTED_AT TIMESTAMP, _EXECUTION_ID VARCHAR, _SOURCE_URL VARCHAR""",
    "STATEMENTS": """
        ID NUMBER, PAY_STATUS NUMBER, BILL_AMT NUMBER, PAY_AMT NUMBER, SOURCE_COLUMNS VARCHAR,
        STATEMENT_MONTH VARCHAR, _INGESTED_AT TIMESTAMP, _EXECUTION_ID VARCHAR, _SOURCE_URL VARCHAR""",
    "DEFAULT_LABELS": """
        ID NUMBER, DEFAULT_PAYMENT_NEXT_MONTH NUMBER, LABEL_MONTH VARCHAR,
        _INGESTED_AT TIMESTAMP, _EXECUTION_ID VARCHAR, _SOURCE_URL VARCHAR""",
}


# --------------------------------------------------------------------------- load
def snowflake_conn_kwargs() -> dict:
    """Autenticación key-pair (SNOWFLAKE_PRIVATE_KEY_PATH) o, si no está, contraseña."""
    kw = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        kw["private_key_file"] = os.path.expanduser(key_path)
    else:
        kw["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return kw


def load_snowflake(parts: dict[str, pd.DataFrame], schema: str) -> dict[str, int]:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    conn = snowflake.connector.connect(**snowflake_conn_kwargs())
    counts = {}
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")
        for table, df in parts.items():
            cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({DDL[table]})")
            part_val = df[PARTITION_COL[table]].iloc[0]
            cur.execute("BEGIN")
            cur.execute(
                f"DELETE FROM {schema}.{table} WHERE {PARTITION_COL[table]} = %s", (part_val,)
            )
            ok, _, nrows, _ = write_pandas(
                conn, df, table, schema=schema, quote_identifiers=False, use_logical_type=True
            )
            if not ok:
                cur.execute("ROLLBACK")
                raise RuntimeError(f"write_pandas falló en {table}")
            cur.execute("COMMIT")
            counts[table] = nrows
            log.info("%s.%s particion %s: %s filas", schema, table, part_val, nrows)
    finally:
        conn.close()
    return counts


def load_duckdb(parts: dict[str, pd.DataFrame], schema: str, path: str) -> dict[str, int]:
    """Destino local equivalente, usado para probar el pipeline sin Snowflake."""
    import duckdb

    con = duckdb.connect(path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    counts = {}
    for table, df in parts.items():
        ddl = DDL[table].replace("NUMBER", "BIGINT")
        con.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({ddl})")
        part_val = df[PARTITION_COL[table]].iloc[0]
        con.execute("BEGIN")
        con.execute(f"DELETE FROM {schema}.{table} WHERE {PARTITION_COL[table]} = ?", [part_val])
        con.register("df_in", df)
        con.execute(f"INSERT INTO {schema}.{table} BY NAME SELECT * FROM df_in")
        con.execute("COMMIT")
        counts[table] = len(df)
    con.close()
    return counts


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--statement-month", required=True, help="YYYY-MM")
    ap.add_argument("--target", choices=["snowflake", "duckdb"], default="snowflake")
    ap.add_argument("--schema", default=os.environ.get("RAW_SCHEMA", "RAW"))
    ap.add_argument("--duckdb-path", default="local.duckdb")
    ap.add_argument("--source-url", default=os.environ.get("UCI_SOURCE_URL", DEFAULT_SOURCE_URL))
    ap.add_argument("--local-file", default=None)
    ap.add_argument("--execution-id", default=os.environ.get("KESTRA_EXECUTION_ID", str(uuid.uuid4())))
    args = ap.parse_args()

    month = args.statement_month[:7]
    if month not in MONTH_COLUMNS:
        # Corrida programada sin partición nueva: no es error, se registra y termina.
        log.warning("La fuente no tiene partición para %s; nada que cargar.", month)
        result = {"statement_month": month, "rows": {}, "status": "NO_PARTITION"}
        print(json.dumps(result))
        _kestra_outputs(result)
        return 0

    df = read_source(args.source_url, args.local_file)
    meta = {
        "_INGESTED_AT": datetime.now(timezone.utc).replace(tzinfo=None),
        "_EXECUTION_ID": args.execution_id,
        "_SOURCE_URL": args.local_file or args.source_url,
    }
    parts = build_partitions(df, month, meta)
    if args.target == "snowflake":
        counts = load_snowflake(parts, args.schema)
    else:
        counts = load_duckdb(parts, args.schema, args.duckdb_path)

    if counts.get("STATEMENTS", 0) == 0:
        raise RuntimeError("La partición quedó vacía después de la carga")
    result = {"statement_month": month, "rows": counts, "status": "LOADED"}
    print(json.dumps(result))
    _kestra_outputs(result)
    return 0


def _kestra_outputs(result: dict) -> None:
    try:
        from kestra import Kestra

        Kestra.outputs(result)
    except ImportError:
        pass


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SourceSchemaError as e:
        log.error("Error de esquema en la fuente (no reintentable): %s", e)
        sys.exit(2)

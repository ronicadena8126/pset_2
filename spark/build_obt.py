"""
Construye la One Big Table (OBT) del proyecto "Anticipar la mora" con Spark.

GRANO: una fila por cliente, observado al cierre de sep-2005 (último mes visible),
con el target = impago en oct-2005. Es exactamente la unidad de predicción del PSet 1.

Entradas (Gold): fct_monthly_statement, fct_default_outcome, dim_customer,
                 dim_month, dim_payment_status
Salida:          OBT.OBT_CUSTOMER_DEFAULT (Snowflake)

Validaciones (si alguna falla, el job termina con error y NO escribe):
  1. n filas OBT == n clientes en dim_customer == n filas en fct_default_outcome
  2. customer_id único en la OBT
  3. cada cliente tiene 6 meses de historia en el pivote (sin nulos en m1..m6)
  4. los joins no cambian la suma de bill_amt ni la tasa de impago

Modos:
  --mode snowflake   lee Gold y escribe OBT en Snowflake (spark-snowflake connector)
  --mode snowflake-py lee GOLD y escribe OBT en Snowflake con el conector de Python
                     (Spark hace todo el procesamiento; útil si no hay acceso a Maven)
  --mode local       lee/escribe parquet (para pruebas sin Snowflake)
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_TABLES = [
    "fct_monthly_statement",
    "fct_default_outcome",
    "dim_customer",
    "dim_month",
    "dim_payment_status",
]
# Atributos sensibles excluidos por equidad (decisión del PSet 1)
EXCLUDED_SENSITIVE = ["sex", "marital_status"]
N_MONTHS = 6


# --------------------------------------------------------------------------- IO
def sf_options(schema: str) -> dict:
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        **(
            {"pem_private_key": "".join(
                l for l in open(os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])).read().splitlines()
                if "PRIVATE KEY" not in l)}
            if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
            else {"sfPassword": os.environ["SNOWFLAKE_PASSWORD"]}
        ),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE", ""),
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": schema,
    }


def _py_conn():
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "kestra", "scripts"))
    import snowflake.connector
    from ingest_uci import snowflake_conn_kwargs

    return snowflake.connector.connect(**snowflake_conn_kwargs())


def read_gold(spark: SparkSession, mode: str, local_dir: str) -> dict[str, DataFrame]:
    out = {}
    conn = _py_conn() if mode == "snowflake-py" else None
    for t in GOLD_TABLES:
        if mode == "snowflake-py":
            pdf = conn.cursor().execute(f"SELECT * FROM GOLD.{t.upper()}").fetch_pandas_all()
            for c in pdf.columns:  # NUMBER(38,0)/decimal -> tipos nativos para Spark
                if pdf[c].dtype == object and len(pdf) and pdf[c].map(type).astype(str).str.contains("Decimal").any():
                    pdf[c] = pdf[c].astype(float)
            df = spark.createDataFrame(pdf)
        elif mode == "snowflake":
            df = (
                spark.read.format("net.snowflake.spark.snowflake")
                .options(**sf_options("GOLD"))
                .option("dbtable", t.upper())
                .load()
            )
        else:
            df = spark.read.parquet(os.path.join(local_dir, f"{t}.parquet"))
        out[t] = df.toDF(*[c.lower() for c in df.columns])  # Snowflake devuelve MAYÚSCULAS
    if conn:
        conn.close()
    return out


def write_obt(df: DataFrame, mode: str, local_dir: str) -> None:
    if mode == "snowflake-py":
        from snowflake.connector.pandas_tools import write_pandas

        schema = os.environ.get("OBT_SCHEMA", "OBT")
        pdf = df.toPandas()
        pdf.columns = [c.upper() for c in pdf.columns]
        conn = _py_conn()
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        ok, _, n, _ = write_pandas(conn, pdf, "OBT_CUSTOMER_DEFAULT", schema=schema,
                                   auto_create_table=True, overwrite=True, quote_identifiers=False)
        conn.close()
        if not ok:
            raise RuntimeError("write_pandas falló al escribir la OBT")
    elif mode == "snowflake":
        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options(os.environ.get("OBT_SCHEMA", "OBT")))
            .option("dbtable", "OBT_CUSTOMER_DEFAULT")
            .mode("overwrite")
            .save()
        )
    else:
        df.write.mode("overwrite").parquet(os.path.join(local_dir, "obt_customer_default.parquet"))


# --------------------------------------------------------------------------- build
def build_obt(g: dict[str, DataFrame]) -> DataFrame:
    fct = g["fct_monthly_statement"]
    months = g["dim_month"].select("month_key", "months_to_label")
    status = g["dim_payment_status"].select("payment_status_key", "delay_bucket")

    # 1) Enriquecer el hecho con dimensiones pequeñas (broadcast: 7 y 12 filas)
    stm = (
        fct.join(F.broadcast(months), "month_key", "inner")
        .join(F.broadcast(status), "payment_status_key", "inner")
        # m1 = sep-2005 (último observado) ... m6 = abr-2005
        .withColumn("m", F.col("months_to_label").cast("int"))
    )

    # 2) Pivot cliente x mes -> columnas por mes (valores fijos 1..6: sin escaneo extra)
    pivot_metrics = {
        "pay_status": "payment_status_key",
        "bill_amt": "bill_amt",
        "pay_amt": "pay_amt",
        "util": "utilization_ratio",
    }
    pivoted = (
        stm.groupBy("customer_key")
        .pivot("m", list(range(1, N_MONTHS + 1)))
        .agg(*[F.first(src).alias(name) for name, src in pivot_metrics.items()])
    )
    # Spark nombra "1_pay_status"; renombramos a "pay_status_m1"
    for m in range(1, N_MONTHS + 1):
        for name in pivot_metrics:
            pivoted = pivoted.withColumnRenamed(f"{m}_{name}", f"{name}_m{m}")

    # 3) Agregados de comportamiento en 6 meses
    w3 = F.col("m") <= 3
    aggs = stm.groupBy("customer_key").agg(
        F.count("*").alias("n_months_history"),
        F.max("months_delay").alias("max_delay_6m"),
        F.sum(F.col("is_delinquent").cast("int")).alias("n_delinquent_months_6m"),
        F.sum(F.when(w3, F.col("is_delinquent").cast("int")).otherwise(0)).alias("n_delinquent_months_3m"),
        F.avg("utilization_ratio").alias("avg_utilization_6m"),
        F.max("utilization_ratio").alias("max_utilization_6m"),
        F.avg("payment_ratio").alias("avg_payment_ratio_6m"),
        F.sum("pay_amt").alias("total_pay_amt_6m"),
        F.avg("bill_amt").alias("avg_bill_amt_6m"),
        F.sum(F.col("is_overlimit").cast("int")).alias("n_overlimit_months_6m"),
        F.sum(F.col("has_credit_balance").cast("int")).alias("n_credit_balance_months_6m"),
        F.sum(
            (F.col("flag_delay_without_balance") | F.col("flag_no_use_with_balance")).cast("int")
        ).alias("n_inconsistent_months_6m"),
        F.sum("bill_amt").alias("_sum_bill_amt"),  # solo para validar joins
    )

    customer = g["dim_customer"].drop(*EXCLUDED_SENSITIVE)
    outcome = g["fct_default_outcome"].select(
        "customer_key", "observation_month_key", "label_month_key", "default_next_month"
    )

    obt = (
        customer.join(pivoted, "customer_key", "left")
        .join(aggs, "customer_key", "left")
        .join(outcome, "customer_key", "left")
        .withColumn("bill_trend_m1_vs_m6", F.col("bill_amt_m1") - F.col("bill_amt_m6"))
        .withColumn(
            "is_delinquent_last_month", (F.col("pay_status_m1") >= 1).cast("int")
        )
    )
    return obt


# --------------------------------------------------------------------------- validate
def validate(obt: DataFrame, g: dict[str, DataFrame]) -> dict:
    n_obt = obt.count()
    n_customers = g["dim_customer"].count()
    n_outcome = g["fct_default_outcome"].count()
    n_distinct = obt.select("customer_id").distinct().count()

    pivot_cols = [f"pay_status_m{m}" for m in range(1, N_MONTHS + 1)]
    missing_months = obt.filter(
        " OR ".join(f"{c} IS NULL" for c in pivot_cols) + " OR n_months_history <> 6"
    ).count()
    null_target = obt.filter(F.col("default_next_month").isNull()).count()

    fct_bill = g["fct_monthly_statement"].agg(F.sum("bill_amt")).first()[0]
    obt_bill = obt.agg(F.sum("_sum_bill_amt")).first()[0]
    rate_gold = g["fct_default_outcome"].agg(F.avg("default_next_month")).first()[0]
    rate_obt = obt.agg(F.avg("default_next_month")).first()[0]

    checks = {
        "rows_obt": n_obt,
        "rows_dim_customer": n_customers,
        "rows_fct_default_outcome": n_outcome,
        "distinct_customer_id": n_distinct,
        "customers_missing_months": missing_months,
        "null_target": null_target,
        "sum_bill_amt_fct": float(fct_bill),
        "sum_bill_amt_obt": float(obt_bill),
        "default_rate_gold": float(rate_gold),
        "default_rate_obt": float(rate_obt),
    }
    errors = []
    if not (n_obt == n_customers == n_outcome):
        errors.append("El número de filas cambió con los joins")
    if n_distinct != n_obt:
        errors.append("Hay customer_id duplicados en la OBT")
    if missing_months:
        errors.append(f"{missing_months} clientes sin 6 meses completos")
    if null_target:
        errors.append(f"{null_target} clientes sin target")
    if abs(float(fct_bill) - float(obt_bill)) > 0.01:
        errors.append("La suma de bill_amt no cuadra: el join duplicó o perdió estados de cuenta")
    if abs(float(rate_gold) - float(rate_obt)) > 1e-9:
        errors.append("La tasa de impago cambió después de los joins")
    checks["errors"] = errors
    return checks


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["snowflake", "snowflake-py", "local"], default="snowflake")
    ap.add_argument("--local-dir", default="./gold_parquet")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("pset2-build-obt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    gold = read_gold(spark, args.mode, args.local_dir)
    obt = build_obt(gold).cache()
    checks = validate(obt, gold)
    for k, v in checks.items():
        print(f"[validation] {k}: {v}")
    if checks["errors"]:
        print("[validation] FALLÓ: no se escribe la OBT", file=sys.stderr)
        return 1

    final = obt.drop("_sum_bill_amt")
    write_obt(final, args.mode, args.local_dir)
    print(f"[ok] OBT escrita: {checks['rows_obt']} filas x {len(final.columns)} columnas")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

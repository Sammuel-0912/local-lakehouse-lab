"""
Bronze 層：把原始 CSV「原封不動」搬進 Iceberg。

設計原則
  1. 不做任何清洗、不轉型別：全部欄位都當字串存。
     → 哪天發現 Silver 的清洗規則寫錯了，可以從 Bronze 重新算，不用回頭找來源系統。
  2. 加上「血緣」欄位：這筆資料從哪個檔案、哪個批次、什麼時候進來的。
  3. 冪等（idempotent）：同一天跑兩次，結果要跟跑一次一樣。
     → 用 _batch_date 分區 + overwritePartitions()，重跑只會覆蓋那一天。

用法：spark-submit bronze_ingest.py --date 2026-11-18
"""
import argparse
from pyspark.sql import SparkSession, functions as F

RAW = "/home/iceberg/data/raw"

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True, help="批次日期 YYYY-MM-DD")
args = ap.parse_args()

spark = SparkSession.builder.appName(f"bronze_ingest_{args.date}").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

# ---------- 機台日誌 ----------
# 預設不推斷型別（inferSchema=false）→ 每個欄位都是 string，保留最原始的樣子
logs = (
    spark.read.option("header", True)
    .csv(f"{RAW}/machine_logs/dt={args.date}/")
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_date", F.lit(args.date).cast("date"))
)

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.machine_logs (
    event_id        STRING,
    event_time      STRING,
    machine_id      STRING,
    status          STRING,
    temperature_c   STRING,
    vibration_mm_s  STRING,
    units_produced  STRING,
    _source_file    STRING,
    _ingested_at    TIMESTAMP,
    _batch_date     DATE
) USING iceberg
PARTITIONED BY (_batch_date)
""")

# overwritePartitions = 只覆蓋這批資料涉及的分區（也就是這一天），其他天不受影響
logs.writeTo("bronze.machine_logs").overwritePartitions()
print(f"[bronze] machine_logs {args.date}: {logs.count()} rows")

# ---------- 機台主檔（小表，每次整張覆蓋）----------
machines = (
    spark.read.option("header", True)
    .csv(f"{RAW}/machines/")
    .withColumn("_ingested_at", F.current_timestamp())
)
machines.writeTo("bronze.machines").using("iceberg").createOrReplace()
print(f"[bronze] machines: {machines.count()} rows")

spark.stop()

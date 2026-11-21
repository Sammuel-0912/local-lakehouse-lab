"""
Silver 層：把 Bronze 的原始字串變成「可信任、型別正確、不重複」的資料。

清洗規則（每一條都對應 generate_data.py 故意製造的髒資料）
  - status        去空白、轉大寫               (" running " → "RUNNING")
  - event_time    同時接受 "空白" 與 "T" 兩種格式
  - temperature_c 轉 double，轉不了就是 NULL   ("N/A" → NULL)
  - 重複資料      同一個 event_id 只留一筆

壞資料不丟掉，而是放進 silver.machine_logs_rejected 並註明原因
  → 可以回頭跟上游溝通「你們的 M00x 感測器一直送 N/A」

冪等：用 MERGE INTO 以 event_id 為鍵 upsert，重跑同一天不會變成兩倍。

用法：spark-submit silver_transform.py --date 2026-11-18
"""
import argparse
from pyspark.sql import SparkSession, Window, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True)
args = ap.parse_args()

spark = SparkSession.builder.appName(f"silver_transform_{args.date}").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

raw = spark.table("bronze.machine_logs").where(F.col("_batch_date") == args.date)

# ---------- 1. 標準化 & 轉型 ----------
typed = raw.select(
    F.trim("event_id").alias("event_id"),
    F.coalesce(
        F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss"),
    ).alias("event_ts"),
    F.nullif(F.trim("machine_id"), F.lit("")).alias("machine_id"),
    F.upper(F.trim("status")).alias("status"),
    F.expr("try_cast(temperature_c AS DOUBLE)").alias("temperature_c"),
    F.expr("try_cast(vibration_mm_s AS DOUBLE)").alias("vibration_mm_s"),
    F.expr("try_cast(units_produced AS INT)").alias("units_produced"),
    "_batch_date",
    "_ingested_at",
)

# ---------- 2. 品質檢查：每筆資料算出「被拒絕的原因」，NULL 代表通過 ----------
reject_reason = (
    F.when(F.col("event_id").isNull(), "missing_event_id")
    .when(F.col("event_ts").isNull(), "bad_event_time")
    .when(F.col("machine_id").isNull(), "missing_machine_id")
    .when(~F.col("status").isin("RUNNING", "IDLE", "ERROR"), "unknown_status")
    .when(F.col("temperature_c").isNull(), "temperature_not_numeric")
    .when(~F.col("temperature_c").between(-40, 300), "temperature_out_of_range")
    .when(F.col("units_produced").isNull(), "units_not_numeric")
    .when(F.col("units_produced") < 0, "negative_units_produced")
)
checked = typed.withColumn("_reject_reason", reject_reason)

good = checked.where(F.col("_reject_reason").isNull()).drop("_reject_reason")
bad = checked.where(F.col("_reject_reason").isNotNull())

# ---------- 3. 去重：同一 event_id 只留最晚進來的那筆 ----------
w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
good = (
    good.withColumn("_rn", F.row_number().over(w))
    .where("_rn = 1")
    .drop("_rn")
    .withColumn("_updated_at", F.current_timestamp())
)

# ---------- 4. 寫入 Silver（MERGE = upsert）----------
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.machine_logs (
    event_id        STRING,
    event_ts        TIMESTAMP,
    machine_id      STRING,
    status          STRING,
    temperature_c   DOUBLE,
    vibration_mm_s  DOUBLE,
    units_produced  INT,
    _batch_date     DATE,
    _ingested_at    TIMESTAMP,
    _updated_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_ts))
""")
# ↑ days(event_ts) 是 Iceberg 的「隱藏分區」：
#   查詢時寫 WHERE event_ts >= '2026-11-19' 就會自動裁剪分區，不需要多一個 dt 欄位

good.createOrReplaceTempView("incoming")
spark.sql(f"""
MERGE INTO silver.machine_logs t
USING incoming s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
-- 這一批以前寫進來、但這次已經不在 incoming 裡的資料（例如被新規則拒絕）→ 刪掉
-- 一定要限定 _batch_date，否則其他天的資料也全部「不在 incoming 裡」，會被一起刪光！
WHEN NOT MATCHED BY SOURCE AND t._batch_date = DATE'{args.date}' THEN DELETE
""")

# ---------- 5. 壞資料隔離區 ----------
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.machine_logs_rejected (
    event_id STRING, event_ts TIMESTAMP, machine_id STRING, status STRING,
    temperature_c DOUBLE, vibration_mm_s DOUBLE, units_produced INT,
    _batch_date DATE, _ingested_at TIMESTAMP, _reject_reason STRING
) USING iceberg
PARTITIONED BY (_batch_date)
""")
bad.writeTo("silver.machine_logs_rejected").overwritePartitions()

# ---------- 6. 機台主檔 ----------
spark.sql("""
CREATE OR REPLACE TABLE silver.machines USING iceberg AS
SELECT trim(machine_id)            AS machine_id,
       trim(machine_name)          AS machine_name,
       trim(line)                  AS line,
       trim(model)                 AS model,
       to_date(install_date)       AS install_date
FROM bronze.machines
""")

n_in, n_good, n_bad = raw.count(), good.count(), bad.count()
print(f"[silver] {args.date}: bronze={n_in} -> silver={n_good}, rejected={n_bad}, "
      f"duplicates_removed={n_in - n_good - n_bad}")
bad.groupBy("_reject_reason").count().orderBy("_reject_reason").show(truncate=False)

spark.stop()

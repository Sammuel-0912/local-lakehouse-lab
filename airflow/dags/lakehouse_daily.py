"""
每日 Lakehouse 管線：原始資料 → Bronze → Silver → Gold（dbt）

    extract_raw ─▶ bronze_ingest ─▶ silver_transform ─▶ dbt_build_gold

每個 task 只做一件事、而且都可以安全重跑（冪等），所以：
  - 失敗時 Airflow 自動 retry 不會造成重複資料
  - 想重算某幾天，直接 backfill 或在 UI 上 Clear 就好
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# {{ ds }} 是 Airflow 的模板變數 = 這次 run 的「邏輯日期」(YYYY-MM-DD)
#   - 排程自動跑的 11-21 那次 run → ds = 2026-11-21
#   - backfill 11-18 那次 run     → ds = 2026-11-18
# 每個 task 都只處理 ds 那一天，這是讓 backfill 能運作的關鍵
SPARK_SUBMIT = (
    "docker exec -w /tmp spark-iceberg "
    "spark-submit --conf spark.app.name=airflow_{{{{ task.task_id }}}}_{{{{ ds }}}} "
    "/home/iceberg/jobs/{job} --date {{{{ ds }}}}"
)
# ↑ 注意：這個字串會先經過 Python 的 .format(job=...)，所以 Jinja 的 {{ }} 要寫成 {{{{ }}}}
#   .format 會把 {{{{ 變成 {{，留給 Airflow 的 Jinja 模板引擎處理

default_args = {
    "owner": "data-lab",
    "retries": 1,                          # 失敗自動重試 1 次
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="lakehouse_daily",
    description="原始機台日誌 → Bronze → Silver → Gold",
    start_date=datetime(2026, 11, 18),
    schedule="@daily",
    catchup=False,                         # 啟用時不自動補跑過去的日子（我們會手動 backfill）
    max_active_runs=1,                     # 一次只跑一天：Silver 的 MERGE 不適合多天同時寫
    default_args=default_args,
    tags=["lab", "lakehouse"],
) as dag:

    # 1. 從「來源系統」取資料。這裡用產生器模擬；真實世界可能是從 API / DB / SFTP 拉檔案
    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command="python /opt/lab/scripts/generate_data.py --days 1 --start {{ ds }}",
    )

    # 2. Bronze：原封不動存進 Iceberg（Spark 在 spark-iceberg 容器裡執行，不是在 Airflow 裡）
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=SPARK_SUBMIT.format(job="bronze_ingest.py"),
    )

    # 3. Silver：清洗、去重、隔離壞資料
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=SPARK_SUBMIT.format(job="silver_transform.py"),
    )

    # 4. Gold：dbt 先測 Silver（source tests）→ 建模型 → 測模型，任何一步失敗就停
    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command="cd /opt/lab/dbt_project && dbt build --no-use-colors",
    )

    extract_raw >> bronze_ingest >> silver_transform >> dbt_build_gold

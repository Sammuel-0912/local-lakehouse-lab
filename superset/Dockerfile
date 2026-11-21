# 自訂 Superset 映像檔 = 官方映像 + 連 Spark Thrift Server 的驅動
FROM apache/superset:5.0.0

USER root
# PyHive 同時是 Hive/Spark 的 DB-API 驅動與 SQLAlchemy dialect（連線字串 hive://...）
# 跟 dbt 用的是同一套驅動 → 兩者都是走 Thrift 10000 Port 把 SQL 交給 Spark
RUN uv pip install --no-cache --python /app/.venv/bin/python "pyhive[hive_pure_sasl]==0.7.0" "thrift==0.20.0"
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
USER superset

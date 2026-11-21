# 自訂 Airflow 映像檔 = 官方映像 + 兩樣工具
FROM apache/airflow:2.11.0-python3.11

# 1) docker CLI：只拿執行檔，從官方 docker:cli 映像直接複製過來（multi-stage COPY），不用 apt 裝一大包
#    搭配掛載的 /var/run/docker.sock，Airflow 就能對 spark-iceberg 容器下 docker exec
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker

# 2) dbt：鎖定跟 dbt 容器相同的版本（不鎖的話 pip 會抓最新版，兩邊行為可能不一樣）
#    裝在獨立的 venv，跟 Airflow 自己的套件隔離（避免版本衝突，這是官方建議做法）
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.0" "dbt-spark[PyHive]==1.9.0" \
 && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt
USER airflow

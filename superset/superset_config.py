# Superset 設定檔（放在 /app/pythonpath，啟動時自動載入）
import os

# 用來加密 session 與資料庫連線密碼；正式環境務必換成隨機長字串並放在 secret 管理
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "lab-only-change-me-in-production")

# Superset 自己的 metadata（使用者、圖表、儀表板定義）存在 SQLite
# 注意：這「不是」分析用的資料；分析資料在 Spark/Iceberg 裡，Superset 只是去查詢
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# 查詢結果快取 5 分鐘：同一張圖重複打開不會每次都叫 Spark 重算
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

# Spark 查詢比資料庫慢，把逾時放寬
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False   # 本地 http 學習用；正式環境應開啟

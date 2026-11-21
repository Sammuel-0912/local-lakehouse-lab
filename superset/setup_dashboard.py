"""
用 Superset REST API 建立「工廠機台 KPI」儀表板（Dashboard as Code）。

可以重複執行：資料庫連線 / 資料集已存在就沿用，圖表與儀表板會先刪掉再重建。

    python3 superset/setup_dashboard.py

Superset 的物件層級：
    Database（怎麼連 Spark） → Dataset（哪一張表） → Chart（怎麼畫） → Dashboard（怎麼排）
"""
import json
import uuid

import requests

BASE = "http://localhost:8088"
DB_NAME = "Lakehouse (Spark Thrift)"
# hive:// 是 PyHive 的 SQLAlchemy dialect；Spark Thrift Server 說的是 HiveServer2 協定
# 跟 dbt 一樣要帶一個使用者名稱（superset@），否則 SASL 握手會失敗
DB_URI = "hive://superset@spark-iceberg:10000/default"
DASH_TITLE = "工廠機台 KPI（Lakehouse Gold 層）"
DASH_SLUG = "factory-kpi"


# ---------------------------------------------------------------- API 小工具
def login() -> requests.Session:
    s = requests.Session()
    tok = s.post(f"{BASE}/api/v1/security/login", json={
        "username": "admin", "password": "admin", "provider": "db", "refresh": True,
    }).json()["access_token"]
    s.headers["Authorization"] = f"Bearer {tok}"
    s.headers["X-CSRFToken"] = s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
    s.headers["Referer"] = BASE
    return s


def find(s, resource, col, value):
    q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}]})
    res = s.get(f"{BASE}/api/v1/{resource}/", params={"q": q}).json()["result"]
    return res[0]["id"] if res else None


def check(r):
    if not r.ok:
        raise RuntimeError(f"{r.request.method} {r.url} -> {r.status_code}: {r.text[:500]}")
    return r.json()


# ---------------------------------------------------------------- 1. Database
def ensure_database(s):
    db_id = find(s, "database", "database_name", DB_NAME)
    if db_id:
        return db_id
    return check(s.post(f"{BASE}/api/v1/database/", json={
        "database_name": DB_NAME,
        "sqlalchemy_uri": DB_URI,
        "expose_in_sqllab": True,     # SQL Lab 裡可以直接下 SQL 查 Lakehouse
        "allow_ctas": False,
        "allow_dml": False,           # BI 只讀：禁止從 Superset 改資料
    }))["id"]


# ---------------------------------------------------------------- 2. Dataset
def ensure_dataset(s, db_id, schema, table):
    q = json.dumps({"filters": [
        {"col": "table_name", "opr": "eq", "value": table},
        {"col": "schema", "opr": "eq", "value": schema},
    ]})
    res = s.get(f"{BASE}/api/v1/dataset/", params={"q": q}).json()["result"]
    if res:
        return res[0]["id"]
    return check(s.post(f"{BASE}/api/v1/dataset/", json={
        "database": db_id, "schema": schema, "table_name": table,
    }))["id"]


# ---------------------------------------------------------------- 3. Charts
def metric(sql, label):
    """ad-hoc 指標：直接寫 SQL 聚合式，不必先在資料集上定義"""
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def create_chart(s, dash_id, ds_id, name, params, query):
    params = {"datasource": f"{ds_id}__table", "adhoc_filters": [], **params}
    # query_context 是「後端要怎麼查」；瀏覽器畫圖時會自己從 params 重新組，
    # 但存一份讓 /api/v1/chart/<id>/data 這種 API（報表、驗證）也能取資料
    qc = {
        "datasource": {"id": ds_id, "type": "table"},
        "force": False,
        "queries": [{"filters": [], "extras": {"where": "", "having": ""},
                     "row_limit": 10000, **query}],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }
    return check(s.post(f"{BASE}/api/v1/chart/", json={
        "slice_name": name,
        "viz_type": params["viz_type"],
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "query_context": json.dumps(qc),
        "dashboards": [dash_id],
    }))["id"]


# ---------------------------------------------------------------- 4. Dashboard 版面
def layout(rows, title):
    """rows = [[(chart_id 或 markdown 字串, 寬度, 高度), ...], ...]；每列寬度加總 = 12"""
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    for r, row in enumerate(rows):
        rid = f"ROW-{r}"
        pos["GRID_ID"]["children"].append(rid)
        pos[rid] = {"type": "ROW", "id": rid, "children": [],
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for c, (item, width, height) in enumerate(row):
            if isinstance(item, str):
                cid = f"MARKDOWN-{r}-{c}"
                pos[cid] = {"type": "MARKDOWN", "id": cid, "children": [],
                            "parents": ["ROOT_ID", "GRID_ID", rid],
                            "meta": {"width": width, "height": height, "code": item}}
            else:
                cid = f"CHART-{r}-{c}"
                pos[cid] = {"type": "CHART", "id": cid, "children": [],
                            "parents": ["ROOT_ID", "GRID_ID", rid],
                            "meta": {"chartId": item, "width": width, "height": height,
                                     "uuid": str(uuid.uuid4())}}
            pos[rid]["children"].append(cid)
    return pos


def main():
    s = login()
    db_id = ensure_database(s)
    kpi = ensure_dataset(s, db_id, "gold", "mart_machine_daily_kpi")
    hourly = ensure_dataset(s, db_id, "gold", "fct_machine_hourly")
    rejected = ensure_dataset(s, db_id, "silver", "machine_logs_rejected")
    print(f"database={db_id} datasets: kpi={kpi} hourly={hourly} rejected={rejected}")

    # 重建儀表板：先刪舊的（連同上面的圖表），確保每次執行結果一致
    old = find(s, "dashboard", "slug", DASH_SLUG)
    if old:
        charts = s.get(f"{BASE}/api/v1/dashboard/{old}/charts").json()["result"]
        for c in charts:
            s.delete(f"{BASE}/api/v1/chart/{c['id']}")
        s.delete(f"{BASE}/api/v1/dashboard/{old}")
    dash_id = check(s.post(f"{BASE}/api/v1/dashboard/", json={
        "dashboard_title": DASH_TITLE, "slug": DASH_SLUG, "published": True,
    }))["id"]

    units = metric("SUM(units_produced)", "總產量")
    avail = metric("AVG(availability_ratio)", "平均稼動率")
    err = metric("AVG(error_ratio)", "平均故障率")
    maint = metric("COUNT(DISTINCT CASE WHEN needs_maintenance THEN machine_id END)", "需保養機台數")
    rej = metric("COUNT(*)", "被隔離筆數")
    temp = metric("AVG(avg_temperature_c)", "平均溫度")

    def big(ds, name, m, fmt):
        return create_chart(s, dash_id, ds, name,
                            {"viz_type": "big_number_total", "metric": m,
                             "y_axis_format": fmt, "header_font_size": 0.4,
                             "subheader_font_size": 0.15},
                            {"metrics": [m], "columns": []})

    c_units = big(kpi, "總產量", units, "SMART_NUMBER")
    c_avail = big(kpi, "平均稼動率", avail, ".1%")
    c_maint = big(kpi, "需保養機台數", maint, "SMART_NUMBER")
    c_rej = big(rejected, "資料品質：被隔離筆數", rej, "SMART_NUMBER")

    # 時間序列：每日產量，依產線分色
    x_date = {"x_axis": "event_date", "time_grain_sqla": "P1D"}
    c_trend = create_chart(s, dash_id, kpi, "每日產量（依產線）", {
        "viz_type": "echarts_timeseries_bar", **x_date,
        "metrics": [units], "groupby": ["production_line"], "stack": "Stack",
        "show_legend": True, "row_limit": 10000, "x_axis_time_format": "%m-%d",
        "rich_tooltip": True,
    }, {"columns": [{"columnType": "BASE_AXIS", "sqlExpression": "event_date",
                     "label": "event_date", "expressionType": "SQL", "timeGrain": "P1D"},
                    "production_line"], "metrics": [units]})

    c_avail_trend = create_chart(s, dash_id, kpi, "每日稼動率 vs 故障率", {
        "viz_type": "echarts_timeseries_line", **x_date,
        "metrics": [avail, err], "groupby": [], "y_axis_format": ".0%",
        "show_legend": True, "markerEnabled": True, "row_limit": 10000,
        "x_axis_time_format": "%m-%d", "rich_tooltip": True,
    }, {"columns": [{"columnType": "BASE_AXIS", "sqlExpression": "event_date",
                     "label": "event_date", "expressionType": "SQL", "timeGrain": "P1D"}],
        "metrics": [avail, err]})

    # 類別長條圖：哪台機台產量最高
    c_by_machine = create_chart(s, dash_id, kpi, "各機台總產量", {
        "viz_type": "echarts_timeseries_bar", "x_axis": "machine_name",
        "metrics": [units], "groupby": [], "orientation": "horizontal",
        "x_axis_sort": "總產量", "x_axis_sort_asc": True, "row_limit": 100,
        "show_legend": False,
    }, {"columns": ["machine_name"], "metrics": [units],
        "orderby": [[units, False]]})

    # 熱力圖：機台 × 一天中的小時 → 平均溫度（找出「哪台、幾點」最熱）
    hour_col = {"expressionType": "SQL", "sqlExpression": "hour(event_hour)", "label": "小時"}
    c_heat = create_chart(s, dash_id, hourly, "平均溫度熱力圖（機台 × 小時）", {
        "viz_type": "heatmap_v2", "x_axis": hour_col, "groupby": "machine_id",
        "metric": temp, "linear_color_scheme": "oranges", "normalize_across": "heatmap",
        "show_legend": True, "show_values": False, "row_limit": 10000,
        "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc", "y_axis_format": ".1f",
    }, {"columns": [hour_col, "machine_id"], "metrics": [temp]})

    # 資料品質：每天被 Silver 擋下的原因
    c_rej_trend = create_chart(s, dash_id, rejected, "資料品質：每日隔離原因", {
        "viz_type": "echarts_timeseries_bar", "x_axis": "_batch_date",
        "time_grain_sqla": "P1D", "metrics": [rej], "groupby": ["_reject_reason"],
        "stack": "Stack", "show_legend": True, "row_limit": 10000,
        "x_axis_time_format": "%m-%d",
    }, {"columns": [{"columnType": "BASE_AXIS", "sqlExpression": "_batch_date",
                     "label": "_batch_date", "expressionType": "SQL", "timeGrain": "P1D"},
                    "_reject_reason"], "metrics": [rej]})

    # 明細表
    cols = ["event_date", "machine_id", "machine_name", "production_line", "units_produced",
            "availability_ratio", "error_ratio", "max_temperature_c", "needs_maintenance"]
    c_table = create_chart(s, dash_id, kpi, "每日機台 KPI 明細", {
        "viz_type": "table", "query_mode": "raw", "all_columns": cols,
        "order_by_cols": [json.dumps(["event_date", False]), json.dumps(["machine_id", True])],
        "row_limit": 1000, "page_length": 20, "include_search": True,
        "table_timestamp_format": "%Y-%m-%d",
    }, {"columns": cols, "orderby": [["event_date", False], ["machine_id", True]]})

    note = (
        "#### 資料怎麼來的\n"
        "原始 CSV → **Bronze** → **Silver**（清洗、隔離壞資料）→ **Gold**（dbt 星狀模型）"
        "，由 Airflow `lakehouse_daily` 每天更新。\n\n"
        "上排三個 KPI 與趨勢圖讀 `gold.mart_machine_daily_kpi`；熱力圖讀 "
        "`gold.fct_machine_hourly`；資料品質讀 `silver.machine_logs_rejected`。"
    )
    rows = [
        [(c_units, 3, 25), (c_avail, 3, 25), (c_maint, 3, 25), (c_rej, 3, 25)],
        [(c_trend, 6, 50), (c_avail_trend, 6, 50)],
        [(c_by_machine, 4, 60), (c_heat, 8, 60)],
        [(c_rej_trend, 6, 50), (note, 6, 50)],
        [(c_table, 12, 70)],
    ]
    check(s.put(f"{BASE}/api/v1/dashboard/{dash_id}", json={
        "position_json": json.dumps(layout(rows, DASH_TITLE)),
        "json_metadata": json.dumps({"color_scheme": "supersetColors",
                                     "refresh_frequency": 0}),
    }))
    print(f"dashboard id={dash_id}: {BASE}/superset/dashboard/{DASH_SLUG}/")


if __name__ == "__main__":
    main()

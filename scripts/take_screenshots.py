"""
用無頭瀏覽器（Playwright + Chromium）擷取 README 用的截圖，存到 docs/images/。

在 Playwright 官方容器裡執行（不用在本機裝瀏覽器）：

    docker run --rm --network host -v "$PWD:/w" -w /w \
      mcr.microsoft.com/playwright/python:v1.49.0-noble \
      bash -c "pip install -q playwright==1.49.0 && python scripts/take_screenshots.py"

前提：Superset(8088)、Airflow(8080)、dbt docs(8082) 都已啟動。
帳密是本地實驗室的預設值（admin/admin）。
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("docs/images")
OUT.mkdir(parents=True, exist_ok=True)


def login(page, url, user="admin", pwd="admin"):
    page.goto(url)
    page.fill("input[name=username]", user)
    page.fill("input[name=password]", pwd)
    page.keyboard.press("Enter")
    page.wait_for_load_state("networkidle")


with sync_playwright() as p:
    browser = p.chromium.launch()

    # 1. Superset 儀表板：等所有圖表查完 Spark 再截整頁
    page = browser.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=1)
    login(page, "http://localhost:8088/login/")
    page.goto("http://localhost:8088/superset/dashboard/factory-kpi/?standalone=1")
    page.wait_for_selector(".dashboard-component-chart-holder", timeout=60_000)
    page.wait_for_function(
        "document.querySelectorAll('.loading').length === 0", timeout=180_000)
    page.wait_for_timeout(3000)  # 等 ECharts 動畫結束
    # 任何一張圖出錯就中止：避免把壞掉的儀表板截進 README
    errors = page.locator(".ant-alert-error, [role=alert]").all_inner_texts()
    if errors:
        raise SystemExit(f"儀表板有圖表出錯：{errors[0][:300]}")
    page.screenshot(path=OUT / "superset_dashboard.png", full_page=True)
    print("superset_dashboard.png")

    # 2. dbt docs：完整 lineage graph
    page = browser.new_page(viewport={"width": 1600, "height": 800})
    page.goto("http://localhost:8082/#!/overview?g_v=1")
    page.wait_for_selector("canvas, #cy", timeout=60_000)
    page.wait_for_timeout(4000)  # 等圖形排版完成
    page.screenshot(path=OUT / "dbt_lineage.png")
    print("dbt_lineage.png")

    # 3. Airflow Grid：四天 backfill 的結果
    page = browser.new_page(viewport={"width": 1600, "height": 900})
    login(page, "http://localhost:8080/login/")
    page.goto("http://localhost:8080/dags/lakehouse_daily/grid")
    page.get_by_text("dbt_build_gold").first.wait_for(timeout=60_000)  # 等 task 列表畫出來
    page.wait_for_timeout(3000)
    page.screenshot(path=OUT / "airflow_grid.png")
    print("airflow_grid.png")

    browser.close()

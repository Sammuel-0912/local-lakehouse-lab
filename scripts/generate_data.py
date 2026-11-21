"""
產生模擬的工廠機台資料，故意混入「髒資料」讓 Silver 層有東西可以清。

輸出：
  data/raw/machines/machines.csv                 機台主檔（之後 Gold 層的維度表）
  data/raw/machine_logs/dt=YYYY-MM-DD/logs.csv   每天一個檔案的感測器日誌

用法：python3 scripts/generate_data.py [--days 3] [--rows 2000] [--start 2026-11-18]
"""
import argparse, csv, random, uuid
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "data" / "raw"

MACHINES = [
    # machine_id, name, line, model, install_date
    ("M001", "CNC 銑床 A", "L1", "Haas VF-2", "2021-03-15"),
    ("M002", "CNC 銑床 B", "L1", "Haas VF-2", "2021-03-15"),
    ("M003", "射出成型機", "L1", "Engel e-mac", "2019-08-01"),
    ("M004", "雷射切割機", "L2", "Trumpf 3030", "2022-01-10"),
    ("M005", "沖壓機 A", "L2", "Amada SDE", "2018-11-20"),
    ("M006", "沖壓機 B", "L2", "Amada SDE", "2020-05-05"),
    ("M007", "焊接機器人", "L3", "Fanuc ARC", "2023-02-14"),
    ("M008", "噴塗機器人", "L3", "ABB IRB", "2023-02-14"),
    ("M009", "組裝線輸送帶", "L3", "Bosch TS2", "2017-07-07"),
    ("M010", "品檢 AOI", "L3", "Koh Young", "2024-06-30"),
]
STATUSES = ["RUNNING"] * 8 + ["IDLE", "ERROR"]


def write_machines():
    out = ROOT / "machines" / "machines.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["machine_id", "machine_name", "line", "model", "install_date"])
        w.writerows(MACHINES)
    print("wrote", out)


def make_row(ts: datetime):
    mid = random.choice(MACHINES)[0]
    status = random.choice(STATUSES)
    temp = round(random.gauss(85 if status == "ERROR" else 60, 8), 2)
    vib = round(abs(random.gauss(2.5 if status == "ERROR" else 0.8, 0.3)), 3)
    output = 0 if status != "RUNNING" else random.randint(5, 20)
    return {
        # 用受 seed 控制的亂數產生 UUID（uuid4() 用的是作業系統亂數，每次都不同）
        "event_id": str(uuid.UUID(int=random.getrandbits(128), version=4)),
        "event_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "machine_id": mid,
        "status": status,
        "temperature_c": str(temp),
        "vibration_mm_s": str(vib),
        "units_produced": str(output),
    }


def dirty(row, rows_so_far):
    """約 6% 的資料被弄髒，模擬真實世界常見的問題。"""
    r = random.random()
    if r < 0.01 and rows_so_far:                      # 1. 重送：上游 retry 造成完全重複
        return dict(random.choice(rows_so_far))
    if r < 0.02:                                     # 2. 缺主鍵
        row["machine_id"] = ""
    elif r < 0.03:                                   # 3. 感測器回傳非數字
        row["temperature_c"] = random.choice(["N/A", "ERR", ""])
    elif r < 0.04:                                   # 4. 物理上不可能的數值
        row["temperature_c"] = str(random.choice([-999, 9999]))
    elif r < 0.045:                                  # 5. 時間格式不一致（ISO 8601 帶 T）
        row["event_time"] = row["event_time"].replace(" ", "T")
    elif r < 0.05:                                   # 6. 大小寫 / 空白不一致
        row["status"] = " " + row["status"].lower() + " "
    elif r < 0.06:                                   # 7. 計數器溢位/重置，產量變負數
        row["units_produced"] = str(-random.randint(1, 20))
    return row


def write_logs(day: datetime, n: int):
    rows = []
    for _ in range(n):
        ts = day + timedelta(seconds=random.randint(0, 86399))
        rows.append(dirty(make_row(ts), rows))
    rows.sort(key=lambda r: r["event_time"])
    out = ROOT / "machine_logs" / f"dt={day:%Y-%m-%d}" / "logs.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print("wrote", out, len(rows), "rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--rows", type=int, default=2000)
    ap.add_argument("--start", default="2026-11-18")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    write_machines()
    start = datetime.strptime(a.start, "%Y-%m-%d")
    for i in range(a.days):
        day = start + timedelta(days=i)
        # 每一天用「seed + 日期」當種子 → 同一天不管單獨跑或跟別天一起跑，內容都完全相同
        # 這模擬了真實來源系統：過去某天的資料是固定的，重抓幾次都一樣
        random.seed(f"{a.seed}-{day:%Y-%m-%d}")
        write_logs(day, a.rows)

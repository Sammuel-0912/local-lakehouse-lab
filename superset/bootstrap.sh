#!/bin/bash
# 啟動前的一次性初始化；每次啟動都跑也沒關係（全部都是冪等的）
set -e
superset db upgrade                                   # 建立/升級 metadata 資料表
superset fab create-admin --username admin --password admin \
  --firstname Lab --lastname Admin --email admin@example.com || true   # 已存在就略過
superset init                                         # 建立預設角色與權限
exec /usr/bin/run-server.sh                           # 官方啟動腳本（gunicorn）

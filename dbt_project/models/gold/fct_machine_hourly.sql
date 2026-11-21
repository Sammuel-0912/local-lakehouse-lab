-- 事實表：描述「發生了什麼」── 粒度（grain）= 一台機台 × 一小時
-- 事實表只放「外鍵 + 可加總的數值」，描述性屬性都去維度表 JOIN
-- 先決定 grain 是維度建模最重要的一步：grain 錯了，後面所有數字都會錯
select
    machine_id,                                         -- FK → dim_machine
    event_date,                                         -- FK → dim_date
    event_hour,
    count(*)                                            as reading_count,
    sum(case when status = 'RUNNING' then 1 else 0 end) as running_count,
    sum(case when status = 'IDLE'    then 1 else 0 end) as idle_count,
    sum(case when status = 'ERROR'   then 1 else 0 end) as error_count,
    sum(units_produced)                                 as units_produced,
    round(avg(temperature_c), 2)                        as avg_temperature_c,
    max(temperature_c)                                  as max_temperature_c,
    round(avg(vibration_mm_s), 3)                       as avg_vibration_mm_s,
    max(vibration_mm_s)                                 as max_vibration_mm_s
from {{ ref('stg_machine_logs') }}
group by machine_id, event_date, event_hour

-- 資料集市（mart）：給報表/儀表板直接用的寬表，已經 JOIN 好維度
-- 商業人員不需要懂 Star Schema，SELECT * 就能用
with daily as (
    select
        machine_id,
        event_date,
        sum(reading_count)   as reading_count,
        sum(running_count)   as running_count,
        sum(error_count)     as error_count,
        sum(units_produced)  as units_produced,
        max(max_temperature_c) as max_temperature_c,
        max(max_vibration_mm_s) as max_vibration_mm_s
    from {{ ref('fct_machine_hourly') }}
    group by machine_id, event_date
)
select
    d.event_date,
    dd.weekday_name,
    m.machine_id,
    m.machine_name,
    m.production_line,
    d.units_produced,
    round(d.running_count / d.reading_count, 4)  as availability_ratio,   -- 稼動率（簡化版）
    round(d.error_count   / d.reading_count, 4)  as error_ratio,
    d.max_temperature_c,
    d.max_vibration_mm_s,
    d.max_temperature_c > 90 or d.max_vibration_mm_s > 3.0 as needs_maintenance
from daily d
join {{ ref('dim_machine') }} m on d.machine_id = m.machine_id
join {{ ref('dim_date') }}    dd on d.event_date = dd.date_day

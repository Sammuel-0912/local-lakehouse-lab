-- 維度表：描述「何時」── 每天一列
-- 用資料涵蓋的日期範圍產生連續日期（date spine），沒資料的日子也會有一列，
-- 這樣報表才看得出「那天機台全停」而不是「那天消失了」
with bounds as (
    select min(event_date) as d0, max(event_date) as d1
    from {{ ref('stg_machine_logs') }}
),
spine as (
    select explode(sequence(d0, d1, interval 1 day)) as date_day from bounds
)
select
    date_day,
    year(date_day)                          as year,
    month(date_day)                         as month,
    dayofmonth(date_day)                    as day,
    date_format(date_day, 'E')              as weekday_name,
    dayofweek(date_day) in (1, 7)           as is_weekend
from spine

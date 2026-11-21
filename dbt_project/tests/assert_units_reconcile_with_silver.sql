-- 對帳測試（reconciliation）：Gold 加總後的產量必須等於 Silver 原始加總
-- 這是抓「JOIN 造成重複」或「GROUP BY 漏資料」最有效的方法
with g as (select sum(units_produced) as u from {{ ref('fct_machine_hourly') }}),
     s as (select sum(units_produced) as u from {{ source('silver', 'machine_logs') }})
select g.u as gold_units, s.u as silver_units
from g cross join s
where g.u <> s.u

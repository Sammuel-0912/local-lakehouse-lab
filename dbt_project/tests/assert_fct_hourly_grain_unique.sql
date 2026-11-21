-- 自訂測試（singular test）：回傳任何一列就算失敗
-- 驗證事實表的 grain：同一台機台同一小時只能有一列
select machine_id, event_hour, count(*) as n
from {{ ref('fct_machine_hourly') }}
group by machine_id, event_hour
having count(*) > 1

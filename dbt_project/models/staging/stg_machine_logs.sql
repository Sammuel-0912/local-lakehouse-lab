-- staging 層：只做「改名、選欄位、輕度衍生」，不做商業邏輯
-- materialized=ephemeral → 不會建表，編譯時變成下游 SQL 裡的一段 CTE
select
    event_id,
    event_ts,
    date_trunc('hour', event_ts)         as event_hour,
    to_date(event_ts)                    as event_date,
    machine_id,
    status,
    temperature_c,
    vibration_mm_s,
    units_produced
from {{ source('silver', 'machine_logs') }}

-- 維度表：描述「誰」── 每台機台一列
-- 維度表放的是會被拿來 GROUP BY / WHERE 的屬性（產線、型號、機齡）
select
    machine_id,
    machine_name,
    production_line,
    machine_model,
    install_date,
    floor(months_between(current_date(), install_date) / 12) as machine_age_years
from {{ ref('stg_machines') }}

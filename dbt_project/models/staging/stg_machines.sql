select
    machine_id,
    machine_name,
    line        as production_line,
    model       as machine_model,
    install_date
from {{ source('silver', 'machines') }}

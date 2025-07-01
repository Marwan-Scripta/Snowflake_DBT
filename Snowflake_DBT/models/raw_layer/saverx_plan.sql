select *
from {{ source ('raw_layer_saverx_plan', 'saverx_plan') }} rs

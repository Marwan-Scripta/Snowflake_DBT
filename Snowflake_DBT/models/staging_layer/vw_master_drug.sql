select *
from {{ source ('staging_layer_master_drug', 'vw_master_drug') }} rs

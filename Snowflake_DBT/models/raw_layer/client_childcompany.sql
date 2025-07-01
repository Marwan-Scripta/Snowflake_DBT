select *
from {{ source ('raw_layer_client_childcompany', 'client_childcompany') }} rs

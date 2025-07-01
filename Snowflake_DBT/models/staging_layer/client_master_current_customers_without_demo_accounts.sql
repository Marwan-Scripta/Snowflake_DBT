select *
from {{ source ('staging_layer_current_customers_without_demo_accounts', 'client_master_current_customers_without_demo_accounts') }} rs

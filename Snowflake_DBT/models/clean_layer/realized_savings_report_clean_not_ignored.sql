select *
from {{ source ('clean_layer_realized_savings_clean_not_ignored', 'realized_savings_report_clean_not_ignored') }} rs



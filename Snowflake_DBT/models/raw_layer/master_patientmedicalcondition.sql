select *
from {{ source ('raw_layer_patient_condition', 'master_patientmedicalcondition') }} rs

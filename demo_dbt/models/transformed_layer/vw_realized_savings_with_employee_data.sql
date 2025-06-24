-- This file is part of the dbt test project to demonstrate the use of dbt for data transformation between prod and dev environments.
-- The code below is a dbt model that creates a view named `vw_realized_savings_with_employee_data`.
-- It pulls data from various sources, including realized savings data and employee demographic information.
-- The model applies transformations to the data, such as uppercasing certain fields, categorizing drug types, and joining with other tables to enrich the data.
-- The final output is a view that can be used for reporting and analysis purposes.
-- This model is tagged for QBR reporting and has metadata for ownership and Jira tracking.
-- The model is designed to be run in a dbt environment, and it uses the `config` function to set model properties such as tags and metadata.
-- The model is part of a larger dbt project that includes various layers such as raw,
-- staging, and transformed layers, and it is intended to be tested when used in both production/testing environment for reporting purposes.


--tagging the below .sql file for QBR reporting
-- and adding metadata for owner and Jira tracking
{{ config(
    tags=["QBR_reporting"],
    meta={"owner": "data_team", "jira": "SI24-3974"}
) }}


--actual code starts here
with realized_savings_with_employee_data as (
SELECT 
    rs.switch_id,
    CASE 
        WHEN rs.system_identified_strategy IS NOT NULL AND LENGTH(rs.system_identified_strategy) > 5 
        THEN UPPER(system_identified_strategy) 
        ELSE UPPER(rs.strategy) 
    END AS strategy,
    
    rs.member_savings,
    rs.plan_savings,
    rs.total_savings,
    rs.switch_open,
    rs.switch_close,
    rs.drug_a_id,
    UPPER(rs.drug_a_name),
    UPPER(rs.drug_a_strength),
    UPPER(rs.drug_a_form),
    UPPER(rs.drug_a_type),
    CASE 
        WHEN md_a.drug_type IN ('C', 'L', 'O', 'm') OR md_a.drug_type IS NULL THEN 'Other'
        WHEN md_a.drug_type IN ('4', '9', 'A', 'G') THEN 'Generic'
        WHEN md_a.drug_type = 'B' THEN 'Brand'
        WHEN md_a.drug_type = 'S' THEN 'Specialty'
        ELSE md_a.drug_type
    END AS drug_a_drug_type_group,
    rs.drug_a_fill_date,
    rs.drug_a_member_cost,
    rs.drug_a_plan_cost,
    rs.drug_a_total_cost,
    rs.drug_b_id,
    UPPER(rs.drug_b_name),
    UPPER(rs.drug_b_strength),
    UPPER(rs.drug_b_form),
    UPPER(rs.drug_b_type),
    CASE 
        WHEN md_b.drug_type IN ('C', 'L', 'O', 'm') OR md_b.drug_type IS NULL THEN 'Other'
        WHEN md_b.drug_type IN ('4', '9', 'A', 'G') THEN 'Generic'
        WHEN md_b.drug_type = 'B' THEN 'Brand'
        WHEN md_b.drug_type = 'S' THEN 'Specialty'
        ELSE md_b.drug_type
    END AS drug_b_drug_type_group,
    rs.drug_b_fill_date,
    rs.drug_b_member_cost,
    rs.drug_b_plan_cost,
    rs.drug_b_total_cost,
    rs.mma_id,
    rs.mmsa_id,
    rs.member_id,
    rs.pbm_history_id,
    rs.recurrence_id,
    rs.client_id,
    cm.org_name as CUSTOMER_NAME,
    rs.drug_b_days_supply,
    rs.drug_b_fill_date_in_active_plan_year,
    UPPER(rs.plan_type),
    rs.plan_name,
    rs.drug_a_quantity,
    rs.drug_a_cost_claim_ref_quantity,
    rs.drug_a_cost_claim_ref_dayssupply,
    rs.s3_file_name,
    rs.s3_row_number,
    rs.row_id,
    CONCAT(rs.switch_id, '-', cm.id) AS unique_switch_id,
    CONCAT(rs.recurrence_id, '-', cm.id) AS unique_recurrence_id,
    CONCAT(rs.member_id, '-', cm.id) AS unique_member_id,
    emp.sex,
    emp.dob,
    emp.city,
    emp.zip,
    emp.state,
    emp.reporting_level_5,
    CAST(DATE_TRUNC('month', rs.drug_b_fill_date) AS DATE) AS drug_b_fill_month,
    CASE 
        WHEN md_a.drug_group = 'ADHD/ANTI-NARCOLEPSY/ANTI-OBESITY/ANOREXIANTS' THEN 'ADHD/ANTI-NARCOLEPSY/ANTI-OBESITY/ANOREXIANTS'
        ELSE md_a.drug_group 
    END AS indication,
    CASE 
        WHEN cc.client_id = 18 AND cc.id =15 THEN 'AHN'
        WHEN cc.client_id = 18 AND cc.id != 15 THEN 'Graham'
        ELSE cc.name 
    END AS child_company_name,
    rs.realised, 
    rs.drug_a_claim_id, 
    claims.ndc
FROM  {{my_dynamic_source('clean_layer_realized_savings_clean_not_ignored','realized_savings_report_clean_not_ignored')}} rs
JOIN
    {{my_dynamic_source('staging_layer_current_customers_without_demo_accounts','client_master_current_customers_without_demo_accounts')}} cm ON (rs.client_id = cm.id) --> replacig existing client master table with this new view to exclude any demo accounts in reports
LEFT JOIN 
    {{my_dynamic_source('raw_layer_saverx_employee','saverx_employee')}} emp ON (rs.member_id = emp.id AND rs.client_id = emp.client_id)
LEFT JOIN 
    {{my_dynamic_source('staging_layer_master_drug','vw_master_drug')}} md_a ON (rs.drug_a_id = md_a.id)
LEFT JOIN 
    {{my_dynamic_source('staging_layer_master_drug','vw_master_drug')}} md_b ON (rs.drug_b_id = md_b.id)
LEFT JOIN 
    {{my_dynamic_source('raw_layer_patient_condition','master_patientmedicalcondition')}} pmc ON (md_a.patient_medical_condition_id = pmc.id)
LEFT JOIN 
    {{my_dynamic_source('raw_layer_saverx_plan','saverx_plan')}} plan ON (emp.health_plan_id = plan.id AND emp.client_id = plan.client_id)
LEFT JOIN 
    {{my_dynamic_source('raw_layer_client_childcompany','client_childcompany')}} cc ON (plan.child_company_id = cc.id AND plan.client_id = cc.client_id)
left join 
    {{my_dynamic_source('raw_layer_saverx_pbmhistory','saverx_pbmhistory')}}  claims on (rs.client_id = claims.client_id and rs.drug_a_claim_id = claims.id)
),

final_realized_savings_with_employee_data as (
    select * from realized_savings_with_employee_data
)

select * from final_realized_savings_with_employee_data


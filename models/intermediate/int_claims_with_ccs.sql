with src as (
    select *
    from {{ ref('int_claims_with_diagnoses') }}
),
mapped as (
    select
        s.*, 
        m.icd10_code as mapped_icd10_code,
        m.approximate as icd9_to_icd10_approximate,
        m.combination as icd9_to_icd10_combination,
        m.scenario as icd9_to_icd10_scenario,
        m.choice_list as icd9_to_icd10_choice_list,
        case when m.icd10_code is null then false else true end as has_icd9_to_icd10_map
    from src s
    left join {{ ref('int_icd9_to_icd10_map') }} m
      on s.icd9_code = m.icd9_code
)

select
    m.*,
    c.default_ccsr_category_ip,
    c.default_ccsr_description_ip,
    c.default_ccsr_category_op,
    c.default_ccsr_description_op,
    c.ccsr_category_1,
    c.ccsr_category_1_description,
    c.ccsr_category_2,
    c.ccsr_category_2_description,
    c.ccsr_category_3,
    c.ccsr_category_3_description,
    c.ccsr_category_4,
    c.ccsr_category_4_description,
    c.ccsr_category_5,
    c.ccsr_category_5_description,
    c.ccsr_category_6,
    c.ccsr_category_6_description,
    case when c.icd10_code is null then false else true end as has_ccsr_match
from mapped m
left join {{ ref('stg_ccs_mappings') }} c
  on m.mapped_icd10_code = c.icd10_code

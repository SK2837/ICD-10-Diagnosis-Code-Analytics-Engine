with src as (
    select *
    from {{ source('raw', 'CCS_MAPPINGS') }}
)

select
    ICD_10_CM_CODE as icd10_code,
    ICD_10_CM_CODE_DESCRIPTION as icd10_description,
    DEFAULT_CCSR_CATEGORY_IP as default_ccsr_category_ip,
    DEFAULT_CCSR_CATEGORY_DESCRIPTION_IP as default_ccsr_description_ip,
    DEFAULT_CCSR_CATEGORY_OP as default_ccsr_category_op,
    DEFAULT_CCSR_CATEGORY_DESCRIPTION_OP as default_ccsr_description_op,
    CCSR_CATEGORY_1 as ccsr_category_1,
    CCSR_CATEGORY_1_DESCRIPTION as ccsr_category_1_description,
    CCSR_CATEGORY_2 as ccsr_category_2,
    CCSR_CATEGORY_2_DESCRIPTION as ccsr_category_2_description,
    CCSR_CATEGORY_3 as ccsr_category_3,
    CCSR_CATEGORY_3_DESCRIPTION as ccsr_category_3_description,
    CCSR_CATEGORY_4 as ccsr_category_4,
    CCSR_CATEGORY_4_DESCRIPTION as ccsr_category_4_description,
    CCSR_CATEGORY_5 as ccsr_category_5,
    CCSR_CATEGORY_5_DESCRIPTION as ccsr_category_5_description,
    CCSR_CATEGORY_6 as ccsr_category_6,
    CCSR_CATEGORY_6_DESCRIPTION as ccsr_category_6_description,
    RATIONALE_FOR_DEFAULT_ASSIGNMENT as default_assignment_rationale
from src
where coalesce(ICD_10_CM_CODE, '') <> ''
qualify row_number() over (
    partition by ICD_10_CM_CODE
    order by ICD_10_CM_CODE_DESCRIPTION
) = 1

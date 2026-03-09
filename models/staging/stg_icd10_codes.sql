with src as (
    select *
    from {{ source('raw', 'ICD10_CODES') }}
)

select
    ICD10_CODE as icd10_code,
    DESCRIPTION as icd10_description
from src
where coalesce(ICD10_CODE, '') <> ''
qualify row_number() over (
    partition by ICD10_CODE
    order by DESCRIPTION
) = 1

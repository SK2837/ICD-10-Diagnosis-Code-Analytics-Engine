with src as (
    select *
    from {{ source('raw', 'CLAIMS_OUTPATIENT') }}
)

select
    md5(concat_ws('|', coalesce(DESYNPUF_ID, ''), coalesce(CLM_ID, ''), coalesce(SEGMENT, ''))) as claim_pk,
    DESYNPUF_ID as beneficiary_id,
    CLM_ID as claim_id,
    SEGMENT as claim_segment,
    try_to_date(CLM_FROM_DT::varchar, 'YYYYMMDD') as claim_from_date,
    try_to_date(CLM_THRU_DT::varchar, 'YYYYMMDD') as claim_thru_date,
    try_to_number(CLM_PMT_AMT) as claim_payment_amount,
    ICD9_DGNS_CD_1 as primary_icd9_diagnosis_code,
    src.*
from src
qualify row_number() over (
    partition by DESYNPUF_ID, CLM_ID
    order by CLM_THRU_DT desc nulls last, CLM_FROM_DT desc nulls last
) = 1

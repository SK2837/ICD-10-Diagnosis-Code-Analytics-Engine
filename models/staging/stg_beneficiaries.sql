with src as (
    select *
    from {{ source('raw', 'BENEFICIARY_SUMMARY') }}
)

select
    md5(coalesce(DESYNPUF_ID, '')) as beneficiary_pk,
    DESYNPUF_ID as beneficiary_id,
    try_to_date(BENE_BIRTH_DT::varchar, 'YYYYMMDD') as birth_date,
    try_to_date(BENE_DEATH_DT::varchar, 'YYYYMMDD') as death_date,
    BENE_SEX_IDENT_CD as sex_code,
    BENE_RACE_CD as race_code,
    BENE_ESRD_IND as esrd_indicator,
    src.*
from src
qualify row_number() over (
    partition by DESYNPUF_ID
    order by BENE_DEATH_DT desc nulls last, BENE_BIRTH_DT desc nulls last
) = 1

with inpatient as (
    select
        claim_pk,
        beneficiary_id,
        claim_id,
        'inpatient' as claim_type,
        claim_from_date,
        claim_thru_date,
        claim_payment_amount,
        ADMTNG_ICD9_DGNS_CD,
        ICD9_DGNS_CD_1,
        ICD9_DGNS_CD_2,
        ICD9_DGNS_CD_3,
        ICD9_DGNS_CD_4,
        ICD9_DGNS_CD_5,
        ICD9_DGNS_CD_6,
        ICD9_DGNS_CD_7,
        ICD9_DGNS_CD_8,
        ICD9_DGNS_CD_9,
        ICD9_DGNS_CD_10
    from {{ ref('stg_claims_inpatient') }}
),
outpatient as (
    select
        claim_pk,
        beneficiary_id,
        claim_id,
        'outpatient' as claim_type,
        claim_from_date,
        claim_thru_date,
        claim_payment_amount,
        ADMTNG_ICD9_DGNS_CD,
        ICD9_DGNS_CD_1,
        ICD9_DGNS_CD_2,
        ICD9_DGNS_CD_3,
        ICD9_DGNS_CD_4,
        ICD9_DGNS_CD_5,
        ICD9_DGNS_CD_6,
        ICD9_DGNS_CD_7,
        ICD9_DGNS_CD_8,
        ICD9_DGNS_CD_9,
        ICD9_DGNS_CD_10
    from {{ ref('stg_claims_outpatient') }}
),
carrier as (
    select
        claim_pk,
        beneficiary_id,
        claim_id,
        'carrier' as claim_type,
        claim_from_date,
        claim_thru_date,
        cast(null as number) as claim_payment_amount,
        cast(null as varchar) as ADMTNG_ICD9_DGNS_CD,
        ICD9_DGNS_CD_1,
        ICD9_DGNS_CD_2,
        ICD9_DGNS_CD_3,
        ICD9_DGNS_CD_4,
        ICD9_DGNS_CD_5,
        ICD9_DGNS_CD_6,
        ICD9_DGNS_CD_7,
        ICD9_DGNS_CD_8,
        cast(null as varchar) as ICD9_DGNS_CD_9,
        cast(null as varchar) as ICD9_DGNS_CD_10
    from {{ ref('stg_claims_carrier') }}
),
claims as (
    select * from inpatient
    union all
    select * from outpatient
    union all
    select * from carrier
),
claim_diagnoses as (
    select
        claim_pk,
        beneficiary_id,
        claim_id,
        claim_type,
        claim_from_date,
        claim_thru_date,
        claim_payment_amount,
        diagnosis_position,
        diagnosis_code
    from claims
    unpivot (
        diagnosis_code for diagnosis_position in (
            ADMTNG_ICD9_DGNS_CD,
            ICD9_DGNS_CD_1,
            ICD9_DGNS_CD_2,
            ICD9_DGNS_CD_3,
            ICD9_DGNS_CD_4,
            ICD9_DGNS_CD_5,
            ICD9_DGNS_CD_6,
            ICD9_DGNS_CD_7,
            ICD9_DGNS_CD_8,
            ICD9_DGNS_CD_9,
            ICD9_DGNS_CD_10
        )
    )
),
cleaned as (
    select
        claim_pk,
        beneficiary_id,
        claim_id,
        claim_type,
        claim_from_date,
        claim_thru_date,
        claim_payment_amount,
        diagnosis_position,
        upper(trim(diagnosis_code)) as diagnosis_code,
        regexp_replace(upper(trim(diagnosis_code)), '[^A-Z0-9]', '') as icd9_code
    from claim_diagnoses
    where coalesce(trim(diagnosis_code), '') <> ''
)
select * from cleaned

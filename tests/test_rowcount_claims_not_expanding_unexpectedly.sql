with staging_claims as (
    select count(distinct claim_pk) as cnt
    from (
        select claim_pk from {{ ref('stg_claims_inpatient') }}
        union all
        select claim_pk from {{ ref('stg_claims_outpatient') }}
        union all
        select claim_pk from {{ ref('stg_claims_carrier') }}
    ) u
),
intermediate_claims as (
    select count(distinct claim_pk) as cnt
    from {{ ref('int_claims_with_diagnoses') }}
),
violations as (
    select
        i.cnt as intermediate_claim_count,
        s.cnt as staging_claim_count
    from intermediate_claims i
    cross join staging_claims s
    where i.cnt > s.cnt
)
select * from violations

with src as (
    select *
    from {{ ref('int_claims_with_hcc') }}
),
claim_level as (
    select
        claim_pk,
        claim_id,
        beneficiary_id,
        claim_type,
        min(claim_from_date) as claim_from_date,
        max(claim_thru_date) as claim_thru_date,
        count(*) as diagnosis_rows,
        count_if(has_icd9_to_icd10_map) as mapped_rows,
        count_if(has_hcc_match) as hcc_matched_rows
    from src
    group by claim_pk, claim_id, beneficiary_id, claim_type
),
beneficiary_hcc_history as (
    select distinct
        beneficiary_id,
        cms_hcc_v28 as hcc_category
    from src
    where has_hcc_match
      and coalesce(cms_hcc_v28, '') <> ''
),
claim_hcc as (
    select distinct
        claim_pk,
        cms_hcc_v28 as hcc_category
    from src
    where has_hcc_match
      and coalesce(cms_hcc_v28, '') <> ''
),
missing_by_claim as (
    select
        cl.claim_pk,
        count(*) as missing_hcc_count,
        listagg(bh.hcc_category, ', ') within group (order by bh.hcc_category) as missing_hcc_categories
    from claim_level cl
    join beneficiary_hcc_history bh
      on cl.beneficiary_id = bh.beneficiary_id
    left join claim_hcc ch
      on cl.claim_pk = ch.claim_pk
     and bh.hcc_category = ch.hcc_category
    where ch.hcc_category is null
    group by cl.claim_pk
)

select
    cl.claim_pk,
    cl.claim_id,
    cl.beneficiary_id,
    cl.claim_type,
    cl.claim_from_date,
    cl.claim_thru_date,
    cl.diagnosis_rows,
    cl.mapped_rows,
    cl.hcc_matched_rows,
    coalesce(m.missing_hcc_count, 0) as missing_hcc_count,
    coalesce(m.missing_hcc_categories, '') as missing_hcc_categories,
    case
        when cl.mapped_rows > 0
         and cl.hcc_matched_rows = 0
         and coalesce(m.missing_hcc_count, 0) > 0 then true
        else false
    end as is_potential_revenue_leakage,
    case
        when cl.mapped_rows = 0 then 'no_icd10_mapping'
        when cl.hcc_matched_rows > 0 then 'hcc_captured'
        when coalesce(m.missing_hcc_count, 0) > 0 then 'missing_hcc_vs_history'
        else 'review_needed'
    end as leakage_reason
from claim_level cl
left join missing_by_claim m
  on cl.claim_pk = m.claim_pk

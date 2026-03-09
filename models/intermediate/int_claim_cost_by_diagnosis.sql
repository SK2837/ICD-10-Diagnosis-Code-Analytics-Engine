with src as (
    select *
    from {{ ref('int_claims_with_hcc') }}
),
counted as (
    select
        src.*,
        count(*) over (partition by claim_pk) as diagnosis_count_in_claim
    from src
),
final as (
    select
        claim_pk,
        claim_id,
        beneficiary_id,
        claim_type,
        claim_from_date,
        claim_thru_date,
        diagnosis_position,
        diagnosis_code,
        mapped_icd10_code,
        has_ccsr_match,
        has_hcc_match,
        ccsr_category_1,
        cms_hcc_v28,
        claim_payment_amount,
        diagnosis_count_in_claim,
        case
            when claim_payment_amount is null then null
            when diagnosis_count_in_claim = 0 then null
            else claim_payment_amount / diagnosis_count_in_claim
        end as allocated_claim_cost
    from counted
)
select * from final

with raw_totals as (
    select
        coalesce(sum(claim_payment_amount), 0) as raw_paid_amount
    from (
        select claim_payment_amount
        from {{ ref('stg_claims_inpatient') }}
        union all
        select claim_payment_amount
        from {{ ref('stg_claims_outpatient') }}
    ) s
),
int_totals as (
    select
        coalesce(sum(allocated_claim_cost), 0) as int_allocated_amount
    from {{ ref('int_claim_cost_by_diagnosis') }}
    where claim_type in ('inpatient', 'outpatient')
),
comparison as (
    select
        r.raw_paid_amount,
        i.int_allocated_amount,
        abs(r.raw_paid_amount - i.int_allocated_amount) as absolute_diff,
        case
            when r.raw_paid_amount = 0 then 0
            else abs(r.raw_paid_amount - i.int_allocated_amount) / r.raw_paid_amount
        end as pct_diff
    from raw_totals r
    cross join int_totals i
)
select *
from comparison
where pct_diff > 0.05

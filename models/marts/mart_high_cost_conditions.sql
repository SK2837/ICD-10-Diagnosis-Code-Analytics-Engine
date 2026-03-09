with condition_costs as (
    select
        coalesce(mapped_icd10_code, diagnosis_code) as diagnosis_code,
        count(*) as diagnosis_rows,
        count(distinct beneficiary_id) as beneficiaries,
        count(distinct claim_pk) as claims,
        sum(coalesce(allocated_claim_cost, 0)) as total_allocated_cost,
        avg(coalesce(allocated_claim_cost, 0)) as avg_allocated_cost
    from {{ ref('int_claim_cost_by_diagnosis') }}
    group by coalesce(mapped_icd10_code, diagnosis_code)
),
scored as (
    select
        *,
        percentile_cont({{ var('high_cost_percentile') }})
            within group (order by total_allocated_cost)
            over () as high_cost_cutoff
    from condition_costs
)

select
    diagnosis_code,
    diagnosis_rows,
    beneficiaries,
    claims,
    total_allocated_cost,
    avg_allocated_cost,
    high_cost_cutoff,
    case when total_allocated_cost >= high_cost_cutoff then true else false end as is_high_cost,
    dense_rank() over (order by total_allocated_cost desc) as cost_rank
from scored

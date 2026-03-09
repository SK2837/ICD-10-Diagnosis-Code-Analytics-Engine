with src as (
    select *
    from {{ ref('int_claim_cost_by_diagnosis') }}
)

select
    coalesce(ccsr_category_1, 'UNMAPPED') as ccsr_category,
    count(*) as diagnosis_rows,
    count(distinct beneficiary_id) as beneficiaries,
    count(distinct claim_pk) as claims,
    sum(coalesce(allocated_claim_cost, 0)) as total_allocated_cost,
    avg(coalesce(allocated_claim_cost, 0)) as avg_allocated_cost,
    count_if(has_ccsr_match) as ccsr_mapped_rows,
    count_if(has_hcc_match) as hcc_mapped_rows
from src
group by coalesce(ccsr_category_1, 'UNMAPPED')

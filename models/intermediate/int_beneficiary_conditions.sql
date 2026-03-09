with src as (
    select *
    from {{ ref('int_claims_with_hcc') }}
)

select
    beneficiary_id,
    count(distinct claim_pk) as total_claims,
    count(*) as total_diagnosis_rows,
    count_if(has_ccsr_match) as matched_ccsr_rows,
    count_if(has_hcc_match) as matched_hcc_rows,
    round(count_if(has_ccsr_match) / nullif(count(*), 0), 6) as ccsr_match_rate,
    round(count_if(has_hcc_match) / nullif(count(*), 0), 6) as hcc_match_rate,
    listagg(distinct ccsr_category_1, ', ') within group (order by ccsr_category_1) as ccsr_categories,
    listagg(distinct cms_hcc_v28, ', ') within group (order by cms_hcc_v28) as hcc_categories
from src
group by beneficiary_id

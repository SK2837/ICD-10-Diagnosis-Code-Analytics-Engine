with src as (
    select *
    from {{ ref('int_claims_with_hcc') }}
)

select
    claim_type,
    count(*) as diagnosis_rows,
    count_if(has_icd9_to_icd10_map) as mapped_to_icd10_rows,
    count_if(has_ccsr_match) as ccsr_matched_rows,
    count_if(has_hcc_match) as hcc_matched_rows,
    round(count_if(has_icd9_to_icd10_map) / nullif(count(*), 0), 6) as icd9_to_icd10_map_rate,
    round(count_if(has_ccsr_match) / nullif(count(*), 0), 6) as ccsr_match_rate,
    round(count_if(has_hcc_match) / nullif(count(*), 0), 6) as hcc_match_rate
from src
group by claim_type
order by claim_type

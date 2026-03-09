with src as (
    select *
    from {{ ref('int_claims_with_ccs') }}
)

select
    s.*,
    h.cms_hcc_esrd_v21,
    h.cms_hcc_esrd_v24,
    h.cms_hcc_v22,
    h.cms_hcc_v24,
    h.cms_hcc_v28,
    h.rxhcc_v05,
    h.rxhcc_v08,
    case when h.icd10_code is null then false else true end as has_hcc_match
from src s
left join {{ ref('stg_hcc_mappings') }} h
  on s.mapped_icd10_code = h.icd10_code

with src as (
    select *
    from {{ source('raw', 'HCC_CROSSWALK') }}
)

select
    DIAGNOSIS_CODE as icd10_code,
    DESCRIPTION as icd10_description,
    CMS_HCC_ESRD_MODEL_CATEGORY_V21 as cms_hcc_esrd_v21,
    CMS_HCC_ESRD_MODEL_CATEGORY_V24 as cms_hcc_esrd_v24,
    CMS_HCC_MODEL_CATEGORY_V22 as cms_hcc_v22,
    CMS_HCC_MODEL_CATEGORY_V24 as cms_hcc_v24,
    CMS_HCC_MODEL_CATEGORY_V28 as cms_hcc_v28,
    RXHCC_MODEL_CATEGORY_V05 as rxhcc_v05,
    RXHCC_MODEL_CATEGORY_V08 as rxhcc_v08
from src
where coalesce(DIAGNOSIS_CODE, '') <> ''
  and regexp_like(DIAGNOSIS_CODE, '^[A-Z0-9\\.]{3,8}$')
qualify row_number() over (
    partition by DIAGNOSIS_CODE
    order by DESCRIPTION
) = 1

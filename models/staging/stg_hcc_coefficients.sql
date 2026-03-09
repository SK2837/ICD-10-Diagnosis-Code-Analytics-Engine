with hcc_categories as (
    select distinct cms_hcc_v28 as hcc_category
    from {{ ref('stg_hcc_mappings') }}
    where coalesce(cms_hcc_v28, '') <> ''
)

select
    hcc_category,
    cast(null as float) as hcc_coefficient
from hcc_categories

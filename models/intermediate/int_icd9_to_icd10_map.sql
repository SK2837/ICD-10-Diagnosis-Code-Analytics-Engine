with gem as (
    select
        upper(trim(icd9_code)) as icd9_code,
        upper(trim(icd10_code)) as icd10_code,
        try_to_number(approximate) as approximate,
        try_to_number(no_map) as no_map,
        try_to_number(combination) as combination,
        try_to_number(scenario) as scenario,
        try_to_number(choice_list) as choice_list
    from {{ ref('icd9_to_icd10_gem') }}
    where coalesce(trim(icd9_code), '') <> ''
      and coalesce(trim(icd10_code), '') <> ''
),
ranked as (
    select
        *,
        row_number() over (
            partition by icd9_code
            order by
                no_map asc,
                approximate asc,
                combination asc,
                scenario asc,
                choice_list asc,
                icd10_code asc
        ) as mapping_rank
    from gem
    where no_map = 0
)
select
    icd9_code,
    icd10_code,
    approximate,
    no_map,
    combination,
    scenario,
    choice_list,
    mapping_rank,
    case when mapping_rank = 1 then true else false end as is_preferred_mapping
from ranked
qualify mapping_rank = 1

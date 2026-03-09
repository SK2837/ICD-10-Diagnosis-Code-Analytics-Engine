with hcc_base as (
    select
        beneficiary_id,
        cms_hcc_v28 as hcc_category
    from {{ ref('int_claims_with_hcc') }}
    where has_hcc_match
      and coalesce(cms_hcc_v28, '') <> ''
),
hcc_distinct as (
    select distinct beneficiary_id, hcc_category
    from hcc_base
),
coeff as (
    select
        hcc_category,
        coalesce(hcc_coefficient, 1.0) as hcc_coefficient
    from {{ ref('stg_hcc_coefficients') }}
),
hcc_scored as (
    select
        d.beneficiary_id,
        d.hcc_category,
        coalesce(c.hcc_coefficient, 1.0) as hcc_coefficient
    from hcc_distinct d
    left join coeff c
      on d.hcc_category = c.hcc_category
),
hcc_totals as (
    select
        beneficiary_id,
        count(*) as unique_hcc_count,
        sum(hcc_coefficient) as hcc_component_score
    from hcc_scored
    group by beneficiary_id
),
demo as (
    select
        beneficiary_id,
        sex_code,
        race_code,
        birth_date,
        datediff(year, birth_date, current_date()) as age_years
    from {{ ref('stg_beneficiaries') }}
),
hcc_freq as (
    select
        beneficiary_id,
        hcc_category,
        count(*) as hcc_occurrences,
        row_number() over (
            partition by beneficiary_id
            order by count(*) desc, hcc_category
        ) as hcc_rank
    from hcc_base
    group by beneficiary_id, hcc_category
),
top_hcc as (
    select
        beneficiary_id,
        listagg(hcc_category, ', ') within group (order by hcc_rank) as top_hccs
    from hcc_freq
    where hcc_rank <= 3
    group by beneficiary_id
),
final as (
    select
        d.beneficiary_id,
        d.sex_code,
        d.race_code,
        d.birth_date,
        d.age_years,
        coalesce(t.unique_hcc_count, 0) as unique_hcc_count,
        coalesce(t.hcc_component_score, 0.0) as hcc_component_score,
        case
            when d.age_years >= 85 then 0.20
            when d.age_years >= 75 then 0.10
            else 0.00
        end
        + case when d.sex_code = '2' then 0.05 else 0.00 end as demographic_adjustment,
        coalesce(t.hcc_component_score, 0.0)
        + case
            when d.age_years >= 85 then 0.20
            when d.age_years >= 75 then 0.10
            else 0.00
          end
        + case when d.sex_code = '2' then 0.05 else 0.00 end as total_hcc_score,
        case
            when coalesce(t.hcc_component_score, 0.0)
                + case
                    when d.age_years >= 85 then 0.20
                    when d.age_years >= 75 then 0.10
                    else 0.00
                  end
                + case when d.sex_code = '2' then 0.05 else 0.00 end < {{ var('risk_tier_low') }}
                then 'low'
            when coalesce(t.hcc_component_score, 0.0)
                + case
                    when d.age_years >= 85 then 0.20
                    when d.age_years >= 75 then 0.10
                    else 0.00
                  end
                + case when d.sex_code = '2' then 0.05 else 0.00 end < {{ var('risk_tier_medium') }}
                then 'medium'
            when coalesce(t.hcc_component_score, 0.0)
                + case
                    when d.age_years >= 85 then 0.20
                    when d.age_years >= 75 then 0.10
                    else 0.00
                  end
                + case when d.sex_code = '2' then 0.05 else 0.00 end < {{ var('risk_tier_high') }}
                then 'high'
            else 'very_high'
        end as risk_tier,
        coalesce(th.top_hccs, '') as top_hccs
    from demo d
    left join hcc_totals t
      on d.beneficiary_id = t.beneficiary_id
    left join top_hcc th
      on d.beneficiary_id = th.beneficiary_id
)
select * from final

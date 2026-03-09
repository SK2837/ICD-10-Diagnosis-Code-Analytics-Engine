with risk as (
    select *
    from {{ ref('mart_patient_risk_scores') }}
),
cohorted as (
    select
        risk_tier,
        sex_code,
        race_code,
        case
            when age_years < 18 then '00-17'
            when age_years < 35 then '18-34'
            when age_years < 50 then '35-49'
            when age_years < 65 then '50-64'
            when age_years < 75 then '65-74'
            when age_years < 85 then '75-84'
            else '85+'
        end as age_band,
        count(*) as beneficiary_count,
        avg(total_hcc_score) as avg_total_hcc_score,
        min(total_hcc_score) as min_total_hcc_score,
        max(total_hcc_score) as max_total_hcc_score
    from risk
    group by risk_tier, sex_code, race_code,
        case
            when age_years < 18 then '00-17'
            when age_years < 35 then '18-34'
            when age_years < 50 then '35-49'
            when age_years < 65 then '50-64'
            when age_years < 75 then '65-74'
            when age_years < 85 then '75-84'
            else '85+'
        end
)

select
    md5(concat_ws('|', coalesce(risk_tier,''), coalesce(sex_code,''), coalesce(race_code,''), coalesce(age_band,''))) as cohort_id,
    *
from cohorted

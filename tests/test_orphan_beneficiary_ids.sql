with orphan_rows as (
    select distinct i.beneficiary_id
    from {{ ref('int_claims_with_hcc') }} i
    left join {{ ref('stg_beneficiaries') }} b
      on i.beneficiary_id = b.beneficiary_id
    where b.beneficiary_id is null
)
select * from orphan_rows

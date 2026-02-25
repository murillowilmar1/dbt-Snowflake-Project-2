with plans as (

    select *
    from {{ ref('stg_saas__plans') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_sk,

    plan_id,
    plan_name,
    billing_period,
    list_price_usd,
    is_active

from plans
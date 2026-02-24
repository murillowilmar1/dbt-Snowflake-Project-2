with source as (

    select *
    from {{ source('raw', 'raw_plans') }}

)

select

    {{ clean_id('plan_id', '_SIN_PLAN') }} as plan_id,

    {{ clean_id('plan_name', '_SIN_PLAN_NAME') }} as plan_name,

    {{ clean_id('billing_period', '_SIN_PERIOD') }} as billing_period,

    {{ cast_amount('list_price_usd') }} as list_price_usd,

    cast(is_active as boolean) as is_active,

    {{ cast_timestamp('updated_at') }} as updated_at

from source
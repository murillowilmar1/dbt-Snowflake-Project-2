with source as (

    select *
    from {{ source('raw', 'raw_subscriptions') }}

)

select

    {{ clean_id('subscription_id', '_SIN_SUBSCRIPTION') }} as subscription_id,

    {{ cast_user_id('user_id') }} as user_id,

    {{ clean_id('plan_id', '_SIN_PLAN') }} as plan_id,

    {{ cast_date('start_date') }} as start_date,

    {{ cast_date('cancel_date') }} as cancel_date,

    {{ clean_id('status', '_SIN_STATUS') }} as status,

    cast(discount_pct as number(5,2)) as discount_pct,

    {{ cast_timestamp('updated_at') }} as updated_at

from source
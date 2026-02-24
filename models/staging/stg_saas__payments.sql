with source as (

    select * 
    from {{ source('raw', 'raw_payments') }}

)

select

    {{ clean_id('payment_id', '_SIN_PAYMENT') }} as payment_id,

    {{ clean_id('subscription_id', '_SIN_SUBSCRIPTION') }} as subscription_id,

    {{ cast_user_id('user_id') }} as user_id,

    {{ cast_date('paid_date') }} as paid_date,

    {{ cast_amount('amount_usd') }} as amount_usd,
    {{ cast_amount('tax_usd') }} as tax_usd,
    {{ cast_amount('discount_usd') }} as discount_usd,

    {{ clean_id('payment_status', '_SIN_STATUS') }} as payment_status,

    {{ cast_timestamp('updated_at') }} as updated_at

from source
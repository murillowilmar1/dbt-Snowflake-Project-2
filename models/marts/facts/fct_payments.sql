{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

with payments as (

    select *
    from {{ ref('int_payments_enriched') }}

),

dim_user as (
    select user_sk, user_id
    from {{ ref('dim_user') }}
),

dim_plan as (
    select plan_sk, plan_id
    from {{ ref('dim_plan') }}
)

select
    p.payment_id,

    u.user_sk,
    pl.plan_sk,

    p.subscription_id,
    p.paid_date,
    p.payment_status,
    p.is_successful_payment,

    p.amount_usd,
    p.discount_usd,
    p.tax_usd,
    p.net_amount_usd,
    p.total_amount_usd,

    current_timestamp() as loaded_at

from payments p
left join dim_user u
    on p.user_id = u.user_id
left join dim_plan pl
    on p.plan_id = pl.plan_id

{% if is_incremental() %}
where p.paid_date > (select max(paid_date) from {{ this }})
{% endif %}
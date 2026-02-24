with payments as (
    select *
    from {{ ref('stg_saas__payments') }}
),

subs as (
    select subscription_id, user_id, plan_id, discount_pct, status
    from {{ ref('stg_saas__subscriptions') }}
),

plans as (
    select plan_id, billing_period, list_price_usd, plan_name
    from {{ ref('stg_saas__plans') }}
)

select
    p.payment_id,
    p.subscription_id,
    p.user_id,
    p.paid_date,
    p.payment_status,
    p.updated_at as payment_updated_at,

    -- amounts
    p.amount_usd,
    p.tax_usd,
    p.discount_usd,

    (p.amount_usd - p.discount_usd) as net_amount_usd,
    (p.amount_usd - p.discount_usd + p.tax_usd) as total_amount_usd,

    -- subscription / plan context
    s.plan_id,
    s.discount_pct,
    s.status as subscription_status,

    pl.plan_name,
    pl.billing_period,
    pl.list_price_usd

from payments p
left join subs s
  on p.subscription_id = s.subscription_id
left join plans pl
  on s.plan_id = pl.plan_id
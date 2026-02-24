with subs as (
    select *
    from {{ ref('stg_saas__subscriptions') }}
),

plans as (
    select *
    from {{ ref('stg_saas__plans') }}
),

users as (
    select *
    from {{ ref('stg_saas__users') }}
)

select
    s.subscription_id,
    s.user_id,
    s.plan_id,
    s.start_date,
    s.cancel_date,
    s.status,
    s.discount_pct,
    s.updated_at as subscription_updated_at,

    p.plan_name,
    p.billing_period,
    p.list_price_usd,
    p.is_active as plan_is_active,
    p.updated_at as plan_updated_at,

    u.email,
    u.full_name,
    u.country,
    u.signup_ts,
    u.source as acquisition_source,
    u.is_employee,
    u.updated_at as user_updated_at

from subs s
left join plans p on s.plan_id = p.plan_id
left join users u on s.user_id = u.user_id
with p as (
    select *
    from {{ ref('int_payments_enriched') }}
),

base as (
    select
        date_trunc('month', paid_date) as mrr_month,
        user_id,
        subscription_id,
        plan_id,
        billing_period,
        list_price_usd,
        discount_pct,

        case
            when billing_period = 'MONTHLY' then list_price_usd
            when billing_period = 'YEARLY' then (list_price_usd / 12)
            else null
        end as mrr_gross_usd
    from p
    where payment_status = 'PAID'
)

select
    mrr_month,
    user_id,

    round(sum(
        mrr_gross_usd * (1 - coalesce(discount_pct, 0))
    ),2) as mrr_net_usd

from base
where mrr_gross_usd is not null
group by 1,2
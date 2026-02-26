{{ config(materialized='view') }}

with mrr as (
    -- fct_mrr tiene user_id + mrr_month + mrr_net_usd (1 fila por usuario/mes)
    select
        mrr_month as month,
        sum(mrr_net_usd) as total_mrr_usd,
        count(distinct user_id) as active_paying_users
    from {{ ref('fct_mrr') }}
    group by 1
),

revenue as (
    select
        date_trunc('month', paid_date) as month,
        sum(case when payment_status = 'PAID' then total_amount_usd else 0 end) as revenue_usd,
        sum(case when payment_status = 'PAID' then net_amount_usd else 0 end) as net_revenue_usd
    from {{ ref('fct_payments') }}
    group by 1
)

select
    coalesce(m.month, r.month) as month,
    coalesce(m.total_mrr_usd, 0) as total_mrr_usd,
    coalesce(r.revenue_usd, 0) as revenue_usd,
    coalesce(r.net_revenue_usd, 0) as net_revenue_usd,
    coalesce(m.active_paying_users, 0) as active_paying_users,
    case
        when coalesce(m.active_paying_users, 0) > 0
        then coalesce(m.total_mrr_usd, 0) / m.active_paying_users
        else 0
    end as arpu
from mrr m
full outer join revenue r
    on m.month = r.month
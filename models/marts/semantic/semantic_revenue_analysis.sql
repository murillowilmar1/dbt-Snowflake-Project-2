{{ config(materialized='view') }}

select
    date_trunc('month', p.paid_date) as month,
    u.country,
    pl.plan_name,
    sum(p.net_amount_usd) as net_revenue,
    sum(p.total_amount_usd) as gross_revenue
from {{ ref('fct_payments') }} p
left join {{ ref('dim_user') }} u
    on p.user_sk = u.user_sk
left join {{ ref('dim_plan') }} pl
    on p.plan_sk = pl.plan_sk
where p.payment_status = 'PAID'
group by 1,2,3
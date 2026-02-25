{{ config(materialized='view') }}

with p as (
    select *
    from {{ ref('fct_payments') }}
)

select
    date_trunc('month', paid_date) as revenue_month,
    sum(case when payment_status = 'PAID' then total_amount_usd else 0 end) as revenue_usd,
    sum(case when payment_status = 'PAID' then net_amount_usd else 0 end) as net_revenue_usd,
    count_if(payment_status = 'PAID') as successful_payments
from p
group by 1
order by 1
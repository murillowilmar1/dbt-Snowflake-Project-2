{{ config(materialized='view') }}

with mrr as (
    select *
    from {{ ref('fct_mrr') }}
),

users as (
    select user_sk, country
    from {{ ref('dim_user') }}
)

select
    m.mrr_month,
    u.country,
    sum(m.mrr_net_usd) as total_mrr_usd,
    count(distinct m.user_sk) as active_paying_users
from mrr m
left join users u
    on m.user_sk = u.user_sk
group by 1,2
order by 1,2
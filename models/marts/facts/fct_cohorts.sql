{{ config(
    materialized='table'
) }}

with users as (

    select
        user_id,
        date_trunc('month', signup_ts) as cohort_month
    from {{ ref('dim_user') }}

),

mrr as (

    select
        user_id,
        mrr_month
    from {{ ref('fct_mrr') }}

),

cohort_activity as (

    select
        u.cohort_month,
        m.mrr_month as activity_month,
        m.user_id
    from users u
    join mrr m
        on u.user_id = m.user_id
)

select
    cohort_month,
    activity_month,

    datediff(month, cohort_month, activity_month) as months_since_signup,

    count(distinct user_id) as active_users

from cohort_activity
group by 1,2,3
order by 1,2
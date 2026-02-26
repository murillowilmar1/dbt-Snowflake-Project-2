{{ config(materialized='view') }}

select
    cohort_month,
    months_since_signup,
    active_users
from {{ ref('fct_cohorts') }}
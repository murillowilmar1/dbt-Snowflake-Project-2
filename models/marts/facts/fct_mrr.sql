{{ config(
    materialized='incremental',
    unique_key=['user_id', 'mrr_month']
) }}

with mrr as (

    select *
    from {{ ref('int_mrr_monthly') }}

),

dim_user as (
    select user_sk, user_id
    from {{ ref('dim_user') }}
)

select
    u.user_sk,
    m.user_id,
    m.mrr_month,
    m.mrr_net_usd,
    current_timestamp() as loaded_at

from mrr m
left join dim_user u
    on m.user_id = u.user_id

{% if is_incremental() %}
where m.mrr_month > (
    select max(t.mrr_month)
    from {{ this }} t
)
{% endif %}
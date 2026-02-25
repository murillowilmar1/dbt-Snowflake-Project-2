with users as (

    select *
    from {{ ref('int_user_current') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,

    user_id,
    email,
    full_name,
    country,
    signup_ts,
    source,
    is_employee

from users
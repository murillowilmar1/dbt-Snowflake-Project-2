with users as (
    select *
    from {{ ref('stg_saas__users') }}
)

select
    user_id,
    email,
    full_name,
    country,
    signup_ts,
    source,
    is_employee,
    updated_at
from users
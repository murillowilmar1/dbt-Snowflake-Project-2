with users as (

    select *
    from {{ ref('stg_saas__users') }}

),

deduped as (

    select *,
        row_number() over (
            partition by user_id
            order by updated_at desc
        ) as rn
    from users

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

from deduped
where rn = 1
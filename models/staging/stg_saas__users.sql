with source as (

    select * 
    from {{ source('raw', 'raw_users') }}

)

select

    {{ cast_user_id('user_id') }} as user_id,

    {{ clean_id('email', '_SIN_EMAIL') }} as email,

    {{ clean_id('full_name', '_SIN_NAME') }} as full_name,

    {{ clean_id('country', '_SIN_COUNTRY') }} as country,

    {{ cast_timestamp('signup_ts') }} as signup_ts,

    {{ clean_id('source', '_SIN_SOURCE') }} as source,

    cast(is_employee as boolean) as is_employee,

    {{ cast_timestamp('updated_at') }} as updated_at

from source
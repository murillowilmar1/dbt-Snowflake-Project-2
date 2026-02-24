with source as (

    select *
    from {{ source('raw', 'raw_events') }}

)

select

    {{ clean_id('event_id', '_SIN_EVENT') }} as event_id,

    {{ cast_user_id('user_id') }} as user_id,

    {{ cast_timestamp('event_ts') }} as event_ts,

    {{ clean_id('event_name', '_SIN_EVENT_NAME') }} as event_name,

    {{ clean_id('session_id', '_SIN_SESSION') }} as session_id,

    {{ clean_id('device_type', '_SIN_DEVICE') }} as device_type,

    {{ clean_id('page', '_SIN_PAGE') }} as page,

    cast(metadata as variant) as metadata,

    {{ cast_timestamp('updated_at') }} as updated_at

from source
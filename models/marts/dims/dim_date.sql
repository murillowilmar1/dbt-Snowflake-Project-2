with dates as (

    select distinct paid_date as date_day
    from {{ ref('int_payments_enriched') }}

)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    date_trunc('month', date_day) as month_start

from dates
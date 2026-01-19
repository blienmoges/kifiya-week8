{{ config(materialized='table') }}

with dates as (
    {{ dbt_date.get_date_dimension("2019-01-01", "2032-12-31") }}
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
    date_day as full_date,
    day_of_week,
    day_name,
    week_of_year,
    month_of_year as month,
    month_name,
    quarter_of_year as quarter,
    year_number as year,
    case when day_of_week in (6, 7) then true else false end as is_weekend
from dates;

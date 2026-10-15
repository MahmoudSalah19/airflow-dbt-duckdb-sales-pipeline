with dates as (
    select unnest(generate_series(date '2016-01-01', date '2017-12-31', interval 1 day))::date as date
)
select
    cast(strftime(date, '%Y%m%d') as integer) as date_key,
    date,
    year(date) as year,
    month(date) as month,
    day(date) as day,
    monthname(date) as month_name,
    dayname(date) as day_name
from dates
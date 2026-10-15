{{ config(tags=['gold']) }}
-- Fails if any opportunity has a negative close_value.
select opportunity_id, close_value
from {{ ref('fct_sales_opportunity') }}
where close_value < 0
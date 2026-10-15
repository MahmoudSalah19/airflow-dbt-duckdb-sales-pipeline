{{ config(tags=['gold']) }}
-- Fails if a Won deal is missing its close_date_key or close_value.
select opportunity_id, close_date_key, close_value
from {{ ref('fct_sales_opportunity') }}
where deal_stage = 'Won'
  and (close_date_key is null or close_value is null)
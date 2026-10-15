{{ config(tags=['gold']) }}
-- Fails if a Prospecting or Engaging deal already has a close_date_key.
select opportunity_id, deal_stage, close_date_key
from {{ ref('fct_sales_opportunity') }}
where deal_stage in ('Prospecting', 'Engaging')
  and close_date_key is not null
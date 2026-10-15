{{ config(tags=['silver']) }}
-- Fails if any opportunity has a close_date earlier than its engage_date.
select opportunity_id, engage_date, close_date
from {{ ref('sales_pipeline') }}
where close_date is not null
  and engage_date is not null
  and close_date < engage_date
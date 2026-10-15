{{ config(tags=['gold']) }}
-- Fails if any product in sales_pipeline has no matching row in dim_products (e.g. name mismatch like GTXPro).
select distinct p.product_name
from {{ ref('sales_pipeline') }} p
left join {{ ref('dim_products') }} d
    on p.product_name = d.product_name
where p.product_name is not null
  and d.product_id is null
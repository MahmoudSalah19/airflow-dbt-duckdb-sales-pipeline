{{ config(tags=['gold']) }}
-- Fails if the fact table row count differs from silver sales_pipeline (joins dropped or duplicated rows).
with fact as (
    select count(*) as n from {{ ref('fct_sales_opportunity') }}
),
src as (
    select count(*) as n from {{ ref('sales_pipeline') }}
)
select fact.n as fact_rows, src.n as source_rows
from fact, src
where fact.n != src.n
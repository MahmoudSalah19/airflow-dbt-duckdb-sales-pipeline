SELECT
p.opportunity_id  ,
ac.account_id,
pr.product_id,
sm.agent_id,
d.date_key AS engage_date_key,
dd.date_key  AS close_date_key ,
p.deal_stage,
p.close_value 
FROM {{ref('sales_pipeline')}} p
LEFT JOIN {{ref('dim_dates')}} d
ON p.engage_date = d."date"
LEFT JOIN {{ref('dim_dates')}} dd
ON p.close_date =dd."date" 
LEFT JOIN {{ref('dim_accounts')}} ac
ON p.account = ac.account_name
LEFT JOIN {{ref('dim_products')}} pr
ON p.product_name  = pr.product_name
LEFT JOIN {{ref('dim_sales_team')}} sm
ON p.sales_agent   = sm.agent_name
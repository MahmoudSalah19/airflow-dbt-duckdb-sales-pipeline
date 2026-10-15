SELECT 
opportunity_id ,
sales_agent ,
case when product ='GTXPro' then 'GTX Pro' else product end as product_name,
account ,
deal_stage ,
engage_date ,
close_date ,
close_value 
FROM {{source('sales_crm','sales_pipeline')}}
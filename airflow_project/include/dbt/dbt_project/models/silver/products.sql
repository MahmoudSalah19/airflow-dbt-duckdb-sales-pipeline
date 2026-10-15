SELECT 
product as product_name,
series as product_series,
sales_price 
FROM {{source("sales_crm","products")}} 
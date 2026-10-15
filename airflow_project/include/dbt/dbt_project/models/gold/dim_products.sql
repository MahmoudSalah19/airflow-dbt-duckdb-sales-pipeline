SELECT 
ROW_NUMBER() OVER(ORDER BY product_name) as product_id,
product_name ,
product_series ,
sales_price 
FROM {{ref('products')}}
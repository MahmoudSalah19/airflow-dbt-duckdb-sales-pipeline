SELECT 
sales_agent ,
manager ,
regional_office 
FROM {{source('sales_crm','sales_team')}}
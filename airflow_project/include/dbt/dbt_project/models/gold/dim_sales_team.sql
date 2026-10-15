SELECT 
ROW_NUMBER() OVER(ORDER BY sales_agent) AS agent_id ,
sales_agent AS agent_name ,
manager AS manager_name ,
regional_office 
FROM {{ref('sales_team')}}
SELECT 
account,
CASE WHEN sector = 'technolgy' THEN 'technology' ELSE sector END AS sector,
year_established,
revenue,
employees,
office_location,
subsidiary_of 
FROM {{source("sales_crm","accounts")}}

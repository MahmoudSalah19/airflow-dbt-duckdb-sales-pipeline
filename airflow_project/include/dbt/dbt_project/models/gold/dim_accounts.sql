SELECT 
    ROW_NUMBER() OVER(ORDER BY account) AS account_id,
    account AS account_name,
    sector ,
    year_established ,
    revenue ,
    employees ,
    office_location ,
    subsidiary_of,
    CASE WHEN EXISTS (
        SELECT 1 FROM {{ ref('accounts') }} c
        WHERE c.subsidiary_of = a.account
    ) THEN 1 ELSE 0 END AS is_parent
FROM {{ref('accounts')}} a



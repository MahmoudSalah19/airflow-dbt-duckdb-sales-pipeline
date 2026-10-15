{{ config(tags=['gold']) }}
-- Fails if is_parent in dim_accounts does not match whether the account actually has subsidiaries.
with actual_parents as (
    select distinct subsidiary_of as account_name
    from {{ ref('dim_accounts') }}
    where subsidiary_of is not null
)
select a.account_name, a.is_parent
from {{ ref('dim_accounts') }} a
left join actual_parents p on a.account_name = p.account_name
where (a.is_parent = 1 and p.account_name is null)
   or (a.is_parent = 0 and p.account_name is not null)
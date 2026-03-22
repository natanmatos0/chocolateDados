select
  c.customer_id,
  cast(c.age as int) as age,
  upper(left(c.gender, 1)) as gender_code,
  (c.loyalty_member = 1) as is_loyalty_member,
  cast(c.join_date as date) as registration_date,
  case 
    when age < 18 then 'Under 18'
    when age between 18 and 35 then 'Young adult'
    when age between 36 and 60 then 'Adult'
    else 'Senior'
  end as age_group,
  CURRENT_TIMESTAMP as proscessed_at 
from {{ref('bronze_customers')}} as c
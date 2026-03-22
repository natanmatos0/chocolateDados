select
  c.customer_id,
  c.age,
  c.gender,
  c.loyalty_member,
  c.join_date
from {{source('raw', 'customers')}} as c
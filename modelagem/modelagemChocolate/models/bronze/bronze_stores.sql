select
  s.store_id,
  s.store_name,
  s.city,
  s.country,
  s.store_type
from {{source('raw', 'stores')}} as s
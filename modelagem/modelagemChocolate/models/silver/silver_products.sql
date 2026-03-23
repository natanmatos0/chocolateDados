select
  p.product_id,
  trim(p.product_name) as product_name,
  upper(p.brand) as brand,
  category,
  cast(p.cocoa_percent as integer) as cocoa_percent,
  p.weight_g,
  cast(p.weight_g / 1000.0 as decimal(10, 3)) as weight_kg
from {{ref('bronze_products')}} as p
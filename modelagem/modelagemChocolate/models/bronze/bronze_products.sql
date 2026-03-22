select
  product_id,
  product_name,
  brand,
  category,
  cocoa_percent,
  weight_g
from {{source('raw', 'products')}}
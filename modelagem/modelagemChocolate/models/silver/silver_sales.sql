select
  v.order_id,
  
  cast(v.order_date as date) as order_date,
  
  product_id,
  store_id,
  customer_id,
  
  cast(v.quantity as integer) as quantity,
  cast(v.unit_price as decimal(10, 2)) as unit_price,
  cast(v.discount as decimal(10, 2)) as discount,

  cast(v.revenue as decimal(10, 2)) as revenue,
  cast(v.cost as decimal(10, 2)) as cost,
  cast(v.profit as decimal(10, 2)) as profit
from {{ref('bronze_sales')}} as v
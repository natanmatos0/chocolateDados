 with sales as (
  select * from {{ref('silver_sales')}}
),
products as (
  select * from {{ref('silver_products')}}
),
stores as (
  select * from {{ref('silver_stores')}}
)

select
  v.order_id,
  v.order_date,
  v.customer_id,
  p.product_name,
  p.category as product_category,
  s.store_name,
  s.city as store_city,
  v.quantity,
  v.revenue,
  v.cost,
  v.profit,
  case
    when v.revenue > 0 then round((v.profit / v.revenue) * 100, 2)
    else 0
  end as margin_percentage
from sales as v
left join products as p on v.product_id = p.product_id
left join stores as s on v.store_id = s.store_id 

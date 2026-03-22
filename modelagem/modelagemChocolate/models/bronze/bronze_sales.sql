SELECT 
    s.order_id,
    s.order_date,
    s.product_id,
    s.store_id,
    s.customer_id,
    s.quantity,
    s.unit_price,
    s.discount,
    s.revenue,
    s.cost,
    s.profit
from {{source('raw', 'sales')}} as s
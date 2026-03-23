select
  p.product_id,
  p.product_name,
  p.brand,
  p.category,
  p.cocoa_percent,
  p.weight_g,
  case
    when p.weight_kg < 0.1 then 'Pequeno (Até 100g)'
    when p.weight_kg between 0.1 and 0.5  then 'Média (100g - 150g)'
    else 'Grande (> 150g)'
  end as weight_category
from {{ref('silver_products')}} as p
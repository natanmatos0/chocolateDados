select 
    trim(s.store_id) as store_id,
    trim(s.store_name) as store_name,
    city,
    country,
    upper(s.store_type) as store_type
    
from {{ref('bronze_stores')}} as s
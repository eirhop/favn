select
  customer_id,
  customer_name,
  segment,
  contact_count,
  case
    when contact_count >= 2 then 'engaged'
    else 'needs_attention'
  end as health_status

from core.customer

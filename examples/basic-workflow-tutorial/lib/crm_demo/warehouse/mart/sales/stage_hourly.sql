select
  date_trunc('hour', occurred_at) as bucket_hour,
  stage,
  count(*) as deal_count,
  sum(amount_cents) as pipeline_amount_cents

from core.sales.opportunity
where occurred_at >= @window_start
  and occurred_at < @window_end
group by
  date_trunc('hour', occurred_at),
  stage

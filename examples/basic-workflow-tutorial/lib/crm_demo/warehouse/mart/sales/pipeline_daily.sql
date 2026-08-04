select
  cast(occurred_at as date) as snapshot_date,
  stage,
  count(*) as deal_count,
  sum(amount_cents) as pipeline_amount_cents

from core.sales.opportunity
where occurred_at >= @window_start
  and occurred_at < @window_end
group by
  cast(occurred_at as date),
  stage

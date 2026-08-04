select
  pipeline.snapshot_date,
  sum(pipeline.deal_count) as deal_count,
  sum(pipeline.pipeline_amount_cents) as pipeline_amount_cents,
  health.customer_count,
  health.engaged_count

from mart.sales.pipeline_daily as pipeline
cross join (
  select
    count(*) as customer_count,
    count(*) filter (where health_status = 'engaged') as engaged_count
  from mart.sales.account_health
) as health
group by
  pipeline.snapshot_date,
  health.customer_count,
  health.engaged_count

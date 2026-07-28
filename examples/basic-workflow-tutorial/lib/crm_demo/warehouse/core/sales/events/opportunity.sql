select
  deal.deal_id as opportunity_id,
  deal.account_id as customer_id,
  account.segment,
  deal.stage,
  deal.amount_cents,
  deal.occurred_at,

  core_metadata(@favn_run_started_at)

from source.deal as deal
inner join source.account as account
  on account.account_id = deal.account_id
where deal.occurred_at >= @window_start
  and deal.occurred_at < @window_end

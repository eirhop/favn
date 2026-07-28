select
  activity.activity_id as engagement_id,
  activity.account_id as customer_id,
  account.segment,
  activity.activity_type as engagement_type,
  activity.occurred_at,

  core_metadata(@favn_run_started_at)

from source.activity as activity
inner join source.account as account
  on account.account_id = activity.account_id
where activity.occurred_at >= @window_start
  and activity.occurred_at < @window_end

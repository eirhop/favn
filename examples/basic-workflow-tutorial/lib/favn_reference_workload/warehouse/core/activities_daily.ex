defmodule FavnReferenceWorkload.Warehouse.Core.ActivitiesDaily do
  @moduledoc "Daily system-agnostic customer activity model."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Source.{ActivitiesDaily, Accounts}

  relation(true)
  depends(ActivitiesDaily)
  depends(Accounts)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  meta(category: :activities, tags: [:core, :daily, :incremental])

  check :required_keys_are_present, at: :before_materialize, on_violation: :fail do
    ~SQL"""
    select
      count(*) filter (where activity_id is null or occurred_at is null) = 0 as passed
    from query()
    """
  end

  query do
    ~SQL"""
    select
      activities.activity_id,
      activities.account_id,
      accounts.segment,
      activities.activity_type,
      activities.occurred_at
    from source.activities_daily as activities
    inner join source.accounts as accounts
      on accounts.account_id = activities.account_id
    where activities.occurred_at >= @window_start
      and activities.occurred_at < @window_end
    """
  end
end

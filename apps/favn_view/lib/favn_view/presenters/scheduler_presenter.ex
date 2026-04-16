defmodule FavnView.Presenters.SchedulerPresenter do
  @moduledoc """
  Stable UI-facing projection for scheduler inspection rows.
  """

  @spec entry(map()) :: map()
  def entry(raw) when is_map(raw) do
    %{
      pipeline_module: inspect(Map.get(raw, :pipeline_module)),
      schedule_id: inspect(Map.get(raw, :schedule_id)),
      cron: Map.get(raw, :cron),
      timezone: Map.get(raw, :timezone)
    }
  end

  @spec entries([map()]) :: [map()]
  def entries(raw_entries) when is_list(raw_entries), do: Enum.map(raw_entries, &entry/1)
end

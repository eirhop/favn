defmodule CrmDemo.Pipelines.ScheduleProbe do
  @moduledoc """
  Fires every ten seconds so schedules can be observed without waiting a day.

  New schedules are inactive until activated with `mix favn.schedules activate`,
  and scheduler execution only happens when the stack is started with
  `mix favn.dev --scheduler`.
  """

  use Favn.Pipeline

  alias CrmDemo.Lifecycle.ScheduleProbe

  pipeline :schedule_probe do
    asset(ScheduleProbe)
    deps(:all)
    window(:daily, anchor: :previous_complete_period, timezone: "Etc/UTC")
    schedule(cron: "*/10 * * * * *", timezone: "Etc/UTC", missed: :skip, overlap: :forbid)
  end
end

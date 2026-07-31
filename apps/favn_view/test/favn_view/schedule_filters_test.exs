defmodule FavnView.ScheduleFiltersTest do
  use ExUnit.Case, async: true

  alias FavnView.ScheduleFilters

  doctest ScheduleFilters, import: true

  defp schedule(activation, runtime) do
    %{activation_state: activation, runtime_state: runtime}
  end

  defp schedules do
    [
      schedule(:enabled, :running),
      schedule(:enabled, :idle),
      schedule(:disabled, :inactive),
      schedule(:pending_activation, :inactive),
      schedule(:enabled, :queued)
    ]
  end

  test "counts every scope over the whole collection" do
    counts =
      schedules()
      |> ScheduleFilters.scope_choices(%{})
      |> Map.new(&{&1.id, &1.count})

    assert counts == %{
             "all" => 5,
             "enabled" => 3,
             "pending_activation" => 1,
             "disabled" => 1,
             "running" => 1,
             "queued" => 1
           }
  end

  test "a scope's count matches what filtering by that scope returns" do
    schedules = schedules()

    for choice <- ScheduleFilters.scope_choices(schedules, %{}) do
      filters = ScheduleFilters.scope_filters(choice.id)
      active = ScheduleFilters.active_scope(filters)

      matching =
        Enum.count(schedules, fn schedule ->
          activation_ok? =
            filters["activation_state"] == "all" or
              to_string(schedule.activation_state) == filters["activation_state"]

          runtime_ok? =
            filters["runtime_state"] == "all" or
              to_string(schedule.runtime_state) == filters["runtime_state"]

          activation_ok? and runtime_ok?
        end)

      assert choice.count == matching,
             "#{choice.id} promised #{choice.count}, filter gave #{matching}"

      assert active == choice.id, "#{choice.id} does not read back from its own filters"
    end
  end

  test "counts are of the collection, not of an already narrowed page" do
    narrowed = %{"activation_state" => "disabled", "runtime_state" => "all"}

    choices = ScheduleFilters.scope_choices(schedules(), narrowed)

    assert Enum.find(choices, &(&1.id == "all")).count == 5
    assert Enum.find(choices, &(&1.id == "disabled")).active?
  end

  test "a combination no scope covers leaves every button unpressed" do
    filters = %{"activation_state" => "enabled", "runtime_state" => "running"}

    assert ScheduleFilters.active_scope(filters) == nil
    refute Enum.any?(ScheduleFilters.scope_choices(schedules(), filters), & &1.active?)
  end

  test "every scope carries an icon and a hint, so colour is never the only signal" do
    for choice <- ScheduleFilters.scope_choices([], %{}) do
      assert is_binary(choice.icon) and choice.icon != ""
      assert is_binary(choice.hint) and choice.hint != ""
    end
  end
end

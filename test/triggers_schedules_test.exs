defmodule Favn.TriggersSchedulesTest do
  use ExUnit.Case

  alias Favn.Triggers.Schedule
  alias Favn.Triggers.Schedules

  defmodule ExampleSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily_oslo,
      cron: "0 2 * * *",
      timezone: "Europe/Oslo",
      missed: :skip,
      overlap: :forbid
    )
  end

  test "fetch/2 resolves a named schedule with module ref" do
    assert {:ok, %Schedule{} = schedule} = Schedules.fetch(ExampleSchedules, :daily_oslo)
    assert schedule.id == :daily_oslo
    assert schedule.ref == {ExampleSchedules, :daily_oslo}
    assert schedule.origin == :named
  end

  test "schedule DSL rejects unknown option keys" do
    assert_raise ArgumentError, ~r/invalid schedule declaration/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleOpts do
        use Favn.Triggers.Schedules

        schedule :daily,
          cron: \"0 2 * * *\",
          unknown: true
      end
      """)
    end
  end

  test "schedule DSL rejects duplicate names" do
    assert_raise ArgumentError, ~r/is already declared/, fn ->
      Code.compile_string("""
      defmodule DuplicateNamedSchedules do
        use Favn.Triggers.Schedules

        schedule :daily, cron: \"0 2 * * *\"
        schedule :daily, cron: \"0 3 * * *\"
      end
      """)
    end
  end
end

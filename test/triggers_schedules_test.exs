defmodule Favn.TriggersSchedulesTest do
  use ExUnit.Case

  alias Favn.Triggers.Schedule
  alias Favn.Triggers.Schedules

  defmodule ExampleSchedules do
    use Favn.Triggers.Schedules

    @daily_cron "0 2 * * *"

    schedule(:daily_oslo,
      cron: @daily_cron,
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

  test "schedule DSL rejects invalid cron, missed, overlap, timezone, and duplicate options" do
    assert_raise ArgumentError, ~r/invalid_schedule_cron/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleCron do
        use Favn.Triggers.Schedules

        schedule :daily, cron: ""
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_cron/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleCronCommaListDoubleComma do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "1,,2 2 * * *"
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_cron/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleCronCommaListLeadingComma do
        use Favn.Triggers.Schedules

        schedule :daily, cron: ",5 2 * * *"
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_cron/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleCronCommaListTrailingComma do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "5, 2 * * *"
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_missed/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleMissed do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "0 2 * * *", missed: :later
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_overlap/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleOverlap do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "0 2 * * *", overlap: :maybe
      end
      """)
    end

    assert_raise ArgumentError, ~r/invalid_schedule_timezone/, fn ->
      Code.compile_string("""
      defmodule InvalidScheduleTimezone do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "0 2 * * *", timezone: "Mars/Phobos"
      end
      """)
    end

    assert_raise ArgumentError, ~r/duplicate_schedule_opts/, fn ->
      Code.compile_string("""
      defmodule DuplicateScheduleOpts do
        use Favn.Triggers.Schedules

        schedule :daily,
          cron: "0 2 * * *",
          cron: "0 3 * * *"
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

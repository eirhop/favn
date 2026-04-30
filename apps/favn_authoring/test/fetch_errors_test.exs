defmodule FavnAuthoring.FetchErrorsTest do
  use ExUnit.Case, async: true

  alias Favn.Pipeline
  alias Favn.Triggers.Schedules

  defmodule InvalidPipeline do
    def __favn_pipeline__, do: :not_a_pipeline_definition
  end

  defmodule BrokenPipeline do
    def __favn_pipeline__, do: raise(RuntimeError, "broken pipeline implementation")
  end

  defmodule EmptySchedules do
    use Favn.Triggers.Schedules
  end

  defmodule InvalidSchedules do
    def __favn_schedule__(_name), do: :not_a_schedule_definition
  end

  defmodule BrokenSchedules do
    def __favn_schedule__(_name), do: raise(RuntimeError, "broken schedule implementation")
  end

  test "pipeline fetch distinguishes missing and invalid definitions" do
    assert {:error, :not_pipeline_module} = Pipeline.fetch(Favn.Window)
    assert {:error, :pipeline_not_defined} = Pipeline.fetch(InvalidPipeline)
  end

  test "pipeline fetch preserves implementation errors" do
    assert_raise RuntimeError, "broken pipeline implementation", fn ->
      Pipeline.fetch(BrokenPipeline)
    end
  end

  test "schedule fetch distinguishes missing and invalid definitions" do
    assert {:error, :not_schedule_module} = Schedules.fetch(Favn.Window, :daily)
    assert {:error, {:schedule_not_found, :daily}} = Schedules.fetch(EmptySchedules, :daily)
    assert {:error, :schedule_not_defined} = Schedules.fetch(InvalidSchedules, :daily)
  end

  test "schedule fetch preserves implementation errors" do
    assert_raise RuntimeError, "broken schedule implementation", fn ->
      Schedules.fetch(BrokenSchedules, :daily)
    end
  end
end

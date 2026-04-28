defmodule Favn.ScheduleTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedule
  alias Favn.Triggers.Schedules

  test "schedule constructors apply default timezone and validate refs" do
    assert {:ok, unresolved} = Schedule.new_inline(cron: "0 3 * * *")
    assert {:ok, resolved} = Schedule.apply_default_timezone(unresolved, "Etc/UTC")
    assert resolved.timezone == "Etc/UTC"

    assert {:error, :not_schedule_module} = Schedules.fetch(Favn.Window, :daily)

    module_name = Module.concat(__MODULE__, "NoSchedules#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Triggers.Schedules
      end
      """,
      "test/schedule_test.exs"
    )

    assert {:error, {:schedule_not_found, :missing}} = Schedules.fetch(module_name, :missing)
  end

  test "schedule cron validation accepts five-field and six-field expressions" do
    assert {:ok, schedule} = Schedule.new_inline(cron: "0 3 * * *")
    assert schedule.cron == "0 3 * * *"

    assert {:ok, schedule} = Schedule.new_inline(cron: "15 */10 * * * *")
    assert schedule.cron == "15 */10 * * * *"

    assert {:error, {:invalid_schedule_cron, "60 * * * * *"}} =
             Schedule.new_inline(cron: "60 * * * * *")

    assert {:error, {:invalid_schedule_cron, "*/0 * * * * *"}} =
             Schedule.new_inline(cron: "*/0 * * * * *")

    assert {:error, {:invalid_schedule_cron, "0 0 0 * * * *"}} =
             Schedule.new_inline(cron: "0 0 0 * * * *")
  end

  test "Schedules.fetch/2 loads valid schedule modules before export checks" do
    module = Module.concat(__MODULE__, "LoadableSchedules#{System.unique_integer([:positive])}")

    compile_loadable_module!(
      module,
      """
      defmodule #{inspect(module)} do
        use Favn.Triggers.Schedules

        schedule :daily, cron: "0 3 * * *", timezone: "Etc/UTC"
      end
      """
    )

    with_unloaded_module(module, fn ->
      assert {:ok, schedule} = Schedules.fetch(module, :daily)
      assert schedule.ref == {module, :daily}
      assert schedule.cron == "0 3 * * *"
      assert schedule.timezone == "Etc/UTC"
    end)
  end

  defp with_unloaded_module(module, fun) when is_atom(module) and is_function(fun, 0) do
    assert {:module, ^module} = Code.ensure_loaded(module)

    :code.purge(module)
    :code.delete(module)

    try do
      fun.()
    after
      assert {:module, ^module} = Code.ensure_loaded(module)
    end
  end

  defp compile_loadable_module!(module, source) when is_atom(module) and is_binary(source) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_schedule_loadable_modules_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    file_path = Path.join(dir, "#{Macro.underscore(Atom.to_string(module))}.ex")
    File.mkdir_p!(Path.dirname(file_path))
    File.write!(file_path, source)

    Code.prepend_path(dir)

    assert {:ok, modules, _diagnostics} =
             Kernel.ParallelCompiler.compile_to_path([file_path], dir, return_diagnostics: true)

    assert module in modules
  end
end

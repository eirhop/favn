defmodule FavnAuthoring.ScheduleDSLTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedules

  test "schedule DSL fetch validates modules and missing schedules" do
    assert {:error, :not_schedule_module} = Schedules.fetch(Favn.Window, :daily)

    module_name = Module.concat(__MODULE__, "NoSchedules#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Triggers.Schedules
      end
      """,
      "test/schedule_dsl_test.exs"
    )

    assert {:error, {:schedule_not_found, :missing}} = Schedules.fetch(module_name, :missing)
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

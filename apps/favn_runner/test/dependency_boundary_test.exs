defmodule FavnRunner.DependencyBoundaryTest do
  use ExUnit.Case, async: true

  test "the runner test build does not start fixture-only applications" do
    deps = FavnRunner.MixProject.project()[:deps]

    for app <- [:favn_authoring, :favn_test_support] do
      dependency = Enum.find(deps, &(elem(&1, 0) == app))
      assert Keyword.get(dep_opts(dependency), :runtime) == false
    end

    app_file = Path.join([Mix.Project.build_path(), "lib/favn_runner/ebin/favn_runner.app"])

    assert {:ok, [{:application, :favn_runner, properties}]} = :file.consult(app_file)
    refute :favn_authoring in Keyword.fetch!(properties, :applications)
    refute :favn_test_support in Keyword.fetch!(properties, :applications)
  end

  defp dep_opts({_app, opts}) when is_list(opts), do: opts
  defp dep_opts({_app, _requirement, opts}) when is_list(opts), do: opts
end

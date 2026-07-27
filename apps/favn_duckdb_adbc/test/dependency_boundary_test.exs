defmodule FavnDuckdbADBC.DependencyBoundaryTest do
  use ExUnit.Case, async: true

  test "the plugin does not start the isolated runner in a consumer application" do
    deps = FavnDuckdbADBC.MixProject.project()[:deps]
    runner_dep = Enum.find(deps, &(elem(&1, 0) == :favn_runner))

    assert Keyword.get(dep_opts(runner_dep), :runtime) == false

    app_file =
      Path.join([Mix.Project.build_path(), "lib/favn_duckdb_adbc/ebin/favn_duckdb_adbc.app"])

    assert {:ok, [{:application, :favn_duckdb_adbc, properties}]} = :file.consult(app_file)
    refute :favn_runner in Keyword.fetch!(properties, :applications)
  end

  defp dep_opts({_app, opts}) when is_list(opts), do: opts
  defp dep_opts({_app, _requirement, opts}) when is_list(opts), do: opts
end

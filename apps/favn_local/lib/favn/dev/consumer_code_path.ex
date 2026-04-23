defmodule Favn.Dev.ConsumerCodePath do
  @moduledoc false

  @runtime_owned_apps MapSet.new([
                        "favn",
                        "favn_authoring",
                        "favn_core",
                        "favn_duckdb",
                        "favn_local",
                        "favn_orchestrator",
                        "favn_runner",
                        "favn_storage_postgres",
                        "favn_storage_sqlite",
                        "favn_test_support"
                      ])

  @spec ebin_paths(keyword()) :: [Path.t()]
  def ebin_paths(opts \\ []) do
    build_path = Keyword.get_lazy(opts, :build_path, fn -> Mix.Project.build_path() end)
    build_path = Path.expand(build_path, File.cwd!())

    build_path
    |> Path.join("lib/*/ebin")
    |> Path.wildcard()
    |> Enum.filter(&File.dir?/1)
    |> Enum.reject(&runtime_owned_ebin?/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp runtime_owned_ebin?(path) when is_binary(path) do
    app =
      path
      |> Path.dirname()
      |> Path.basename()

    MapSet.member?(@runtime_owned_apps, app)
  end
end

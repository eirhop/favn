defmodule FavnAuthoring.RuntimeRoots do
  @moduledoc """
  Collects executable runtime roots from one compiled manifest build.

  The collector is intentionally limited to authoring-owned information. Runner
  plugins, supervised children, dependency applications, and BEAM traversal are
  added by runner build tooling.
  """

  alias Favn.Manifest.Build
  alias Favn.RunnerRelease.RuntimeRoots

  @doc "Returns Elixir-asset and runtime-input resolver roots."
  @spec collect(Build.t()) :: {:ok, RuntimeRoots.t()} | {:error, RuntimeRoots.error()}
  def collect(%Build{} = build) do
    RuntimeRoots.new(%{
      asset_modules: elixir_asset_modules(build.manifest),
      runtime_input_resolver_modules: resolver_modules(build.execution_packages)
    })
  end

  defp elixir_asset_modules(manifest) do
    manifest
    |> field(:assets, [])
    |> List.wrap()
    |> Enum.filter(&(field(&1, :type) == :elixir))
    |> Enum.map(&field(&1, :module))
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp resolver_modules(packages) do
    packages
    |> Enum.map(fn package ->
      package
      |> field(:sql_execution)
      |> field(:runtime_inputs)
      |> field(:module)
    end)
    |> Enum.filter(&(is_atom(&1) and &1 not in [nil, true, false]))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp field(nil, _key), do: nil

  defp field(value, key) when is_map(value),
    do: Map.get(value, key) || Map.get(value, to_string(key))

  defp field(_value, _key), do: nil

  defp field(value, key, default) do
    case field(value, key) do
      nil -> default
      result -> result
    end
  end
end

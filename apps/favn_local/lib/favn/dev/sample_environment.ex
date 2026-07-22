defmodule Favn.Dev.SampleEnvironment do
  @moduledoc false

  alias Favn.Dev.Paths

  @paths %{
    "FAVN_LOCAL_SAMPLE_DATABASE_PATH" => "local_smoke.duckdb",
    "FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH" => "raw.duckdb",
    "FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH" => "mart.duckdb"
  }

  @spec install_defaults(map(), keyword()) :: map()
  def install_defaults(env_file, opts) when is_map(env_file) and is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    data_dir = Paths.data_dir(root_dir)

    defaults =
      Map.new(@paths, fn {key, filename} ->
        {key, Path.join(data_dir, filename)}
      end)

    loaded =
      Enum.reduce(defaults, %{}, fn {key, value}, acc ->
        if System.get_env(key) == nil do
          System.put_env(key, value)
          Map.put(acc, key, value)
        else
          acc
        end
      end)

    effective_defaults =
      Map.new(defaults, fn {key, value} -> {key, System.get_env(key) || value} end)

    %{
      env_file
      | loaded: Map.merge(env_file.loaded, loaded),
        effective:
          Map.merge(effective_defaults, env_file.effective, fn _key, _default, configured ->
            configured
          end)
    }
  end
end

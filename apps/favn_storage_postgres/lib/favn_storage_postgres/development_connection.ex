defmodule FavnStoragePostgres.DevelopmentConnection do
  @moduledoc false

  @type identity :: :runtime | :migrator

  @spec env_for(identity(), map()) :: {:ok, map()} | {:error, {:missing_env, String.t()}}
  def env_for(:runtime, env) when is_map(env) do
    with {:ok, _url} <- required_url(env, "FAVN_DATABASE_URL") do
      {:ok, env}
    end
  end

  def env_for(:migrator, env) when is_map(env) do
    with {:ok, url} <- required_url(env, "FAVN_DATABASE_MIGRATOR_URL") do
      {:ok, Map.put(env, "FAVN_DATABASE_URL", url)}
    end
  end

  defp required_url(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_env, name}}
    end
  end
end

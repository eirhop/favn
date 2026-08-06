defmodule Mix.Tasks.Favn.Postgres.ReleaseHelpers do
  @moduledoc false

  alias FavnStoragePostgres.DevelopmentConnection

  @spec migrator_env!() :: map() | no_return()
  def migrator_env! do
    case DevelopmentConnection.env_for(:migrator, System.get_env()) do
      {:ok, env} -> env
      {:error, {:missing_env, name}} -> Mix.raise("missing required environment variable #{name}")
    end
  end

  @spec report({:ok, map()} | {:error, map()}, String.t()) :: :ok | no_return()
  def report({:ok, result}, message) do
    Mix.shell().info("#{message}: #{inspect(result)}")
    :ok
  end

  def report({:error, error}, _message) do
    Mix.raise("PostgreSQL release operation failed: #{inspect(error)}")
  end
end

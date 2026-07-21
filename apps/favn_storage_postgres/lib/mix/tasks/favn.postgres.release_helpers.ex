defmodule Mix.Tasks.Favn.Postgres.ReleaseHelpers do
  @moduledoc false

  @spec report({:ok, map()} | {:error, map()}, String.t()) :: :ok | no_return()
  def report({:ok, result}, message) do
    Mix.shell().info("#{message}: #{inspect(result)}")
    :ok
  end

  def report({:error, error}, _message) do
    Mix.raise("PostgreSQL release operation failed: #{inspect(error)}")
  end
end

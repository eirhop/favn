defmodule FavnOrchestrator.ReleaseHealth do
  @moduledoc """
  Container-local readiness probe for the Orchestrator release.

  The check calls the public readiness facade inside the resident Orchestrator
  BEAM and does not depend on the independently scalable View role.
  """

  @doc "Checks the resident Orchestrator readiness snapshot."
  @spec run() :: :ok | {:error, :not_ready}
  def run do
    case FavnOrchestrator.readiness() do
      %{status: :ready} -> :ok
      _not_ready -> {:error, :not_ready}
    end
  end

  @doc "Runs the readiness check and raises a bounded error for release scripts."
  @spec run!() :: :ok
  def run! do
    case run() do
      :ok -> :ok
      {:error, reason} -> raise "orchestrator readiness check failed: #{reason}"
    end
  end
end

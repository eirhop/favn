defmodule Favn.Dev.Diagnostics do
  @moduledoc """
  Local operator diagnostics wrapper for a running Favn stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @type opts :: [root_dir: Path.t()]

  @doc """
  Fetches service-authenticated orchestrator diagnostics for the local stack.
  """
  @spec fetch(opts()) :: {:ok, map()} | {:error, term()}
  def fetch(opts \\ []) when is_list(opts) do
    with :ok <- ensure_orchestrator_reachable(opts),
         {:ok, runtime} <- State.read_runtime(opts),
         {:ok, secrets} <- State.read_secrets(opts),
         {:ok, service_token} <- service_token(secrets) do
      OrchestratorClient.diagnostics(base_url(runtime, opts), service_token)
    end
  end

  defp ensure_orchestrator_reachable(opts) do
    status = Status.inspect_stack(opts)

    cond do
      status.stack_status == :running ->
        :ok

      get_in(status, [:services, :orchestrator, :status]) == :running ->
        :ok

      status.stack_status == :partial ->
        {:error, :orchestrator_not_running}

      true ->
        {:error, :stack_not_running}
    end
  end

  defp service_token(%{"service_token" => token}) when is_binary(token) and token != "",
    do: {:ok, token}

  defp service_token(_secrets), do: {:error, :missing_service_token}

  defp base_url(runtime, opts) do
    runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url
  end
end

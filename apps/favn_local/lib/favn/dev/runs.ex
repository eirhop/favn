defmodule Favn.Dev.Runs do
  @moduledoc """
  Local run inspection helpers for a running `mix favn.dev` stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.LocalContext
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @type run_filters :: [root_dir: Path.t(), status: String.t() | atom(), limit: pos_integer()]
  @type event_filters :: [root_dir: Path.t(), limit: pos_integer(), after_sequence: non_neg_integer()]

  @doc """
  Lists persisted runs from the local orchestrator API.
  """
  @spec list(run_filters()) :: {:ok, [map()]} | {:error, term()}
  def list(opts \\ []) when is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.list_runs(
        base_url,
        credentials.service_token,
        session_context,
        filters(opts, [:status, :limit])
      )
    end
  end

  @doc """
  Fetches one persisted run from the local orchestrator API.
  """
  @spec get(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def get(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.get_run(base_url, credentials.service_token, session_context, run_id)
    end
  end

  @doc """
  Lists persisted run events from the local orchestrator API.
  """
  @spec events(String.t(), event_filters()) :: {:ok, [map()]} | {:error, term()}
  def events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.list_run_events(
        base_url,
        credentials.service_token,
        session_context,
        run_id,
        filters(opts, [:limit, :after_sequence])
      )
    end
  end

  defp session(opts) do
    with :ok <- ensure_running(opts),
         {:ok, runtime} <- State.read_runtime(opts) do
      {:ok, base_url(runtime, opts), LocalContext.credentials(), LocalContext.session_context()}
    end
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      _other -> {:error, :stack_not_running}
    end
  end

  defp base_url(runtime, opts) do
    runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url
  end

  defp filters(opts, allowed) do
    opts
    |> Keyword.take(allowed)
    |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
  end
end

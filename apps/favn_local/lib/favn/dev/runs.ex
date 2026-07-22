defmodule Favn.Dev.Runs do
  @moduledoc """
  Local run inspection and cancellation helpers for a running `mix favn.dev`
  stack.

  This module backs `mix favn.runs list`, `mix favn.runs show RUN_ID`,
  `mix favn.runs cancel RUN_ID`, and the run-event mode of `mix favn.logs RUN_ID`.
  """

  alias Favn.Dev.ComposeSession
  alias Favn.Dev.OrchestratorClient

  @type run_filters :: [root_dir: Path.t(), status: String.t() | atom(), limit: pos_integer()]
  @type event_filters :: [root_dir: Path.t(), limit: pos_integer(), after_sequence: non_neg_integer()]
  @type cancel_opts :: [
          root_dir: Path.t(),
          wait: boolean(),
          timeout_ms: pos_integer(),
          wait_timeout_ms: pos_integer(),
          poll_interval_ms: pos_integer()
        ]

  @terminal_statuses ["ok", "partial", "error", "cancelled", "timed_out"]
  @default_wait_timeout_ms 60_000
  @default_poll_interval_ms 1_000

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
  Requests cancellation for one persisted run through the local orchestrator API.

  Pass `wait: true` to poll the cancelled run until it reaches a terminal status
  or the local wait timeout expires. The polling path fetches only the requested
  run by id.
  """
  @spec cancel(String.t(), cancel_opts()) :: {:ok, map()} | {:error, term()}
  def cancel(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    with :ok <- validate_cancel_opts(opts),
         {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, cancel_result} <-
           OrchestratorClient.cancel_run(
             base_url,
             credentials.service_token,
             run_id,
             session_context
           ) do
      maybe_wait_for_cancel(run_id, cancel_result, base_url, credentials, session_context, opts)
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

  defp session(opts), do: ComposeSession.resolve(opts)

  defp filters(opts, allowed) do
    opts
    |> Keyword.take(allowed)
    |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
  end

  defp validate_cancel_opts(opts) do
    with :ok <- validate_positive_integer(opts, :timeout_ms),
         :ok <- validate_positive_integer(opts, :wait_timeout_ms),
         :ok <- validate_positive_integer(opts, :poll_interval_ms) do
      :ok
    end
  end

  defp validate_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  defp maybe_wait_for_cancel(run_id, cancel_result, base_url, credentials, session_context, opts) do
    if Keyword.get(opts, :wait, false) do
      wait_for_terminal_run(
        run_id,
        base_url,
        credentials.service_token,
        session_context,
        wait_deadline(opts),
        Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms),
        opts
      )
    else
      {:ok, cancel_result}
    end
  end

  defp wait_deadline(opts), do: System.monotonic_time(:millisecond) + wait_timeout_ms(opts)

  defp wait_timeout_ms(opts),
    do:
      Keyword.get(
        opts,
        :wait_timeout_ms,
        Keyword.get(opts, :timeout_ms, @default_wait_timeout_ms)
      )

  defp wait_for_terminal_run(
         run_id,
         base_url,
         service_token,
         session_context,
         deadline,
         poll_interval_ms,
         opts
       ) do
    with {:ok, run} <- OrchestratorClient.get_run(base_url, service_token, session_context, run_id) do
      if terminal_status?(run) do
        {:ok, run}
      else
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          {:error, {:run_wait_timeout, run_id, wait_timeout_ms(opts)}}
        else
          Process.sleep(min(poll_interval_ms, max(deadline - now, 0)))

          wait_for_terminal_run(
            run_id,
            base_url,
            service_token,
            session_context,
            deadline,
            poll_interval_ms,
            opts
          )
        end
      end
    end
  end

  defp terminal_status?(run), do: run_status(run) in @terminal_statuses

  defp run_status(run), do: Map.get(run, "status") || Map.get(run, :status)
end

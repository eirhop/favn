defmodule Favn.Runtime.Engine do
  @moduledoc """
  Runtime engine facade for asynchronous run submission and observation.
  """

  @default_poll_interval_ms 50

  alias Favn.Runtime.Manager

  @spec submit_run(Favn.asset_ref() | [Favn.asset_ref()], keyword()) ::
          {:ok, Favn.run_id()} | {:error, term()}
  def submit_run(target_refs, opts \\ []) when is_list(opts) do
    Manager.submit_run(target_refs, opts)
  end

  @spec cancel_run(Favn.run_id()) ::
          {:ok, :cancelling | :cancelled | :already_terminal}
          | {:error,
             :not_found
             | :invalid_run_id
             | :coordinator_unavailable
             | :timeout_in_progress
             | term()}
  def cancel_run(run_id) do
    Manager.cancel_run(run_id)
  end

  @spec rerun_run(Favn.run_id(), keyword()) :: {:ok, Favn.run_id()} | {:error, term()}
  def rerun_run(run_id, opts \\ [])

  def rerun_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    Manager.rerun_run(run_id, opts)
  end

  def rerun_run(_run_id, _opts), do: {:error, :invalid_run_id}

  @spec await_run(Favn.run_id(), keyword()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def await_run(run_id, opts \\ []) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, :infinity)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    start_ms = System.monotonic_time(:millisecond)

    do_await_run(run_id, start_ms, timeout, poll_interval_ms)
  end

  defp do_await_run(run_id, start_ms, timeout, poll_interval_ms) do
    case Favn.Storage.get_run(run_id) do
      {:ok, %Favn.Run{status: :running}} ->
        if timed_out?(start_ms, timeout) do
          {:error, :timeout}
        else
          Process.sleep(poll_interval_ms)
          do_await_run(run_id, start_ms, timeout, poll_interval_ms)
        end

      {:ok, %Favn.Run{} = run} ->
        if run.status == :ok, do: {:ok, run}, else: {:error, run}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp timed_out?(_start_ms, :infinity), do: false

  defp timed_out?(start_ms, timeout_ms) when is_integer(timeout_ms) and timeout_ms >= 0 do
    System.monotonic_time(:millisecond) - start_ms >= timeout_ms
  end
end

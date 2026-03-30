defmodule Favn.Runtime.Engine do
  @moduledoc """
  Runtime engine facade for asynchronous run submission and observation.
  """

  @default_poll_interval_ms 50

  @spec submit_run(Favn.asset_ref(), keyword()) :: {:ok, Favn.run_id()} | {:error, term()}
  def submit_run(target_ref, opts \\ []) when is_list(opts) do
    Favn.Runtime.Manager.submit_run(target_ref, opts)
  end

  @spec cancel_run(Favn.run_id()) ::
          {:ok, :cancelling | :cancelled | :already_terminal}
          | {:error, :not_found | :invalid_run_id | term()}
  def cancel_run(run_id) do
    Favn.Runtime.Manager.cancel_run(run_id)
  end

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
        if run.status in [:ok, :cancelled], do: {:ok, run}, else: {:error, run}

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

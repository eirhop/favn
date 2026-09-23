defmodule Favn.SQLAsset.MaterializationRetry do
  @moduledoc false
  alias Favn.SQL.{Deadline, Error}

  @spec run(keyword(), (keyword() -> term())) :: term()
  def run(opts, attempt) do
    deadline =
      Deadline.from_opts(
        opts,
        Application.get_env(:favn_sql_runtime, :sql_operation_timeout_ms, 30_000)
      )

    run(attempt, Keyword.put(opts, :deadline, deadline), deadline, 1, nil)
  end

  defp run(attempt, opts, deadline, number, previous) do
    case attempt.(opts) do
      {:supported, {:error, %Error{} = error}} ->
        if Error.rejected_transaction?(error),
          do: retry(attempt, opts, deadline, number, error),
          else: terminal(error, number, previous)

      {_, result} when elem(result, 0) == :ok ->
        result

      {_, {:error, %Error{} = error}} ->
        terminal(error, number, previous)

      {:error, %Error{} = error} ->
        terminal(error, number, previous)

      {support, result} when support in [:supported, :unsupported] ->
        result

      result ->
        result
    end
  end

  defp retry(attempt, opts, deadline, number, error) do
    base = 50 * Integer.pow(2, number - 1)
    delay = base + :rand.uniform(base + 1) - 1

    cond do
      number >= 4 ->
        stop(error, number, "attempt_limit")

      Deadline.remaining_ms(deadline) <= delay ->
        stop(error, number, "deadline")

      true ->
        :telemetry.execute(
          [:favn, :sql_asset, :transaction_retry],
          %{attempt: number, delay_ms: delay, remaining_ms: Deadline.remaining_ms(deadline)},
          opts
          |> Keyword.get(:runtime_publication, %{})
          |> Kernel.||(%{})
          |> Map.take([:publication_id, :run_id, :step_id, :attempt, :asset_ref])
          |> Map.put(:conflict_type, error.type)
        )

        Process.sleep(delay)

        if Deadline.expired?(deadline),
          do: stop(error, number, "deadline"),
          else: run(attempt, opts, deadline, number + 1, error)
    end
  end

  defp terminal(error, _number, nil), do: {:error, error}

  defp terminal(error, number, previous) do
    details = Map.put(error.details, :transaction_retry_attempts, number)

    details =
      if error.details[:session_phase] == :acquiring do
        details
        |> Map.put(:transaction_outcome, :rolled_back)
        |> Map.put(:prior_rejection, Map.take(previous, [:type, :message, :details]))
      else
        details
      end

    {:error, %{error | retryable?: false, details: details}}
  end

  defp stop(error, number, reason) do
    details =
      error.details
      |> Map.put(:transaction_retry_attempts, number)
      |> Map.put(:transaction_retry_stop, reason)

    {:error, %{error | retryable?: false, details: details}}
  end
end

defmodule FavnOrchestrator.RunServer.RecoveryAttention do
  @moduledoc false
  alias FavnOrchestrator.{OperationalEvents, Runs, RunState}
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.Storage.JsonSafe

  @key "recovery_attention"
  @repeat_seconds 60

  @doc false
  @spec record(RunState.t(), term()) :: :ok
  def record(run, reason) do
    context = SystemContext.workspace(run.workspace_id, :run_recovery)

    with {:ok, latest} <- Runs.get(context, run.id),
         false <- RunState.finalized?(latest) do
      now = DateTime.utc_now()
      previous = Map.get(latest.metadata, @key, %{})
      diagnostic = JsonSafe.error(reason)
      identity = JsonSafe.error(original_reason(reason))

      fingerprint =
        :crypto.hash(:sha256, :erlang.term_to_binary({phase(reason), identity}))
        |> Base.encode16()

      last_at = previous["last_reported_at"]

      recent? =
        is_binary(last_at) and last_at > DateTime.to_iso8601(DateTime.add(now, -@repeat_seconds))

      unless previous["fingerprint"] == fingerprint and recent? do
        attention = %{
          "first_seen_at" => previous["first_seen_at"] || DateTime.to_iso8601(now),
          "first_reason" => previous["first_reason"] || diagnostic,
          "phase" => phase(reason),
          "last_reason" => diagnostic,
          "reports" => Map.get(previous, "reports", 0) + 1,
          "fingerprint" => fingerprint,
          "last_reported_at" => DateTime.to_iso8601(now),
          "immediate_retry_budget_ms" => 30_000
        }

        annotated =
          %{
            latest
            | storage_owner_id: run.storage_owner_id,
              storage_fencing_token: run.storage_fencing_token
          }
          |> RunState.transition(metadata: Map.put(latest.metadata, @key, attention))

        result = Persistence.persist_run_step(annotated, :run_recovery_required, attention)
        emit(run, diagnostic, result)
      end
    else
      true ->
        :ok

      {:error, persistence_error} ->
        emit(run, JsonSafe.error(reason), {:error, persistence_error})
    end

    :ok
  end

  defp phase(%{details: details}),
    do: to_string(details[:operation] || details["operation"] || :recovery)

  defp phase({operation, _}), do: to_string(operation)
  defp phase({operation, _, _}), do: to_string(operation)
  defp phase(_), do: "recovery"
  defp original_reason(%{details: details} = reason), do: details[:original_error] || reason
  defp original_reason(reason), do: reason

  defp emit(run, diagnostic, result) do
    OperationalEvents.emit(
      :run_execution_recovery_required,
      %{},
      %{
        workspace_id: run.workspace_id,
        run_id: run.id,
        reason: diagnostic,
        attention_persistence: if(result == :ok, do: :saved, else: JsonSafe.error(result))
      },
      level: :error
    )
  end
end

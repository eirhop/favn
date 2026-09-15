defmodule FavnOrchestrator.Logs.Lifecycle do
  @moduledoc "Renders bounded lifecycle log entries from authoritative step events."

  alias Favn.Log.Entry
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunEventCodec

  @max_metadata_bytes 32 * 1_024
  @error_types ~w(step_failed step_timed_out step_cancelled step_blocked)
  @warning_types ~w(step_retry_scheduled)

  @doc "Event types with non-default lifecycle severity, shared with persistence filters."
  @spec types(Entry.level()) :: [String.t()]
  def types(:error), do: @error_types
  def types(:warning), do: @warning_types
  def types(_level), do: []

  @doc "Returns lifecycle severity; other valid step events are informational."
  @spec level(atom() | String.t()) :: Entry.level()
  def level(type) do
    case to_string(type) do
      type when type in @error_types -> :error
      type when type in @warning_types -> :warning
      _ -> :info
    end
  end

  @doc "Renders one persisted event, or a bounded error identifying the invalid event."
  @spec render(String.t(), pos_integer(), pos_integer(), map()) ::
          {:ok, Entry.t()} | {:error, term()}
  def render(workspace_id, event_id, publication_id, payload) do
    with {:ok, %{entity: :step} = event} <- RunEventCodec.decode(Jason.encode!(payload)),
         true <- valid_identities?(event),
         attrs <-
           event |> RunEvent.from_map() |> entry_attrs() |> Redaction.redact_operational_bounded(),
         true <- is_map(attrs) and byte_size(Jason.encode!(attrs.metadata)) <= @max_metadata_bytes do
      {:ok,
       Entry.normalize(
         Map.merge(attrs, %{
           id: "#{workspace_id}:event:#{event.run_id}:#{event.sequence}",
           global_sequence: (publication_id - 1) * 1_000 + 1
         })
       )}
    else
      _ -> {:error, {:invalid_lifecycle_event, workspace_id, event_id}}
    end
  rescue
    _ -> {:error, {:invalid_lifecycle_event, workspace_id, event_id}}
  end

  defp valid_identities?(event) do
    Enum.all?(
      [{"log_node_key", event.data["node_key"]}, {"log_asset_ref", event.asset_ref}],
      fn {key, original} ->
        case Map.fetch(event.data, key) do
          :error -> is_nil(original)
          {:ok, value} -> is_binary(value) and byte_size(value) in 1..512
        end
      end
    )
  end

  defp entry_attrs(%RunEvent{} = event) do
    %{
      run_id: event.run_id,
      asset_step_id: data_field(event, :asset_step_id),
      node_key: data_field(event, :log_node_key),
      asset_ref: data_field(event, :log_asset_ref),
      runner_task_id: data_field(event, :runner_task_id),
      attempt: data_field(event, :attempt),
      occurred_at: event.occurred_at,
      level: level(event.event_type),
      source: :orchestrator,
      message: transition_log_message(event.event_type),
      metadata: transition_log_metadata(event),
      producer_id: "orchestrator:#{event.run_id}",
      producer_sequence: event.sequence
    }
  end

  defp transition_log_message(:step_started), do: "asset execution submitted"
  defp transition_log_message(:step_retry_started), do: "asset execution retry submitted"
  defp transition_log_message(:step_running), do: "asset execution started on a runner"
  defp transition_log_message(:step_finished), do: "asset execution finished"
  defp transition_log_message(:step_failed), do: "asset execution failed"
  defp transition_log_message(:step_timed_out), do: "asset execution timed out"
  defp transition_log_message(:step_cancelled), do: "asset execution cancelled"
  defp transition_log_message(:step_retry_scheduled), do: "asset execution retry scheduled"
  defp transition_log_message(:step_skipped_fresh), do: "asset skipped because it is fresh"
  defp transition_log_message(:step_blocked), do: "asset execution blocked"

  defp transition_log_message(event_type) when is_atom(event_type),
    do: event_type |> Atom.to_string() |> String.replace("_", " ")

  defp transition_log_message(event_type), do: to_string(event_type)

  defp transition_log_metadata(%RunEvent{} = event) do
    %{
      event_type: event.event_type,
      status: event.status,
      stage: event.stage,
      attempt: data_field(event, :attempt),
      max_attempts: data_field(event, :max_attempts),
      freshness_key: data_field(event, :freshness_key),
      result_status: data_field(event, :result_status),
      error: JsonSafe.error(data_field(event, :error)),
      reason: JsonSafe.error(data_field(event, :reason))
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp data_field(%RunEvent{data: data}, key) when is_map(data) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end
end

defmodule FavnOrchestrator.Rebuild.Telemetry do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Rebuild.Plan

  @spec plan(map(), String.t(), (-> term())) :: term()
  def plan(context, target_id, operation) when is_function(operation, 0) do
    started_at = System.monotonic_time()
    result = operation.()

    {measurements, metadata} =
      case result do
        {:ok, %Plan{payload: payload}} ->
          {
            %{
              duration: System.monotonic_time() - started_at,
              action_count: count(payload, :actions),
              item_count: field(payload, :item_count, count(payload, :items)),
              window_count:
                field(
                  payload,
                  :window_count,
                  field(payload, :item_count, count(payload, :items))
                )
            },
            %{outcome: :ok}
          }

        {:error, reason} ->
          {
            %{duration: System.monotonic_time() - started_at},
            %{outcome: :error, reason: reason_kind(reason)}
          }
      end

    execute(:plan, measurements, Map.merge(metadata, identifiers(context, target_id)))
    result
  end

  @spec execute(atom(), map(), map()) :: :ok
  def execute(event, measurements, metadata)
      when is_atom(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute([:favn, :orchestrator, :rebuild, event], measurements, metadata)
  end

  @spec reason_kind(term()) :: atom()
  def reason_kind(%Error{kind: kind}), do: kind
  def reason_kind(reason) when is_atom(reason), do: reason
  def reason_kind(_reason), do: :unknown

  defp identifiers(context, target_id) do
    %{workspace_id: Map.get(context, :workspace_id), target_id: target_id}
  end

  defp count(payload, key) do
    case field(payload, key, []) do
      values when is_list(values) -> length(values)
      _value -> 0
    end
  end

  defp field(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

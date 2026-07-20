defmodule FavnOrchestrator.RunReadModel.AssetAttemptProjection do
  @moduledoc """
  Normalizes one persisted step event into a compact asset-attempt projection.

  The projection preserves the asset-step identity and concrete runtime window
  without retaining the complete event, run snapshot, or attached plan.
  """

  alias FavnOrchestrator.WindowSummary

  @step_statuses %{
    "step_queued" => :queued,
    "step_started" => :running,
    "step_retry_started" => :running,
    "step_retry_scheduled" => :retrying,
    "step_finished" => :ok,
    "step_failed" => :error,
    "step_timed_out" => :timed_out,
    "step_cancelled" => :cancelled,
    "step_skipped_fresh" => :skipped_fresh,
    "step_blocked" => :blocked
  }

  @type t :: %{
          required(:asset_step_id) => String.t(),
          required(:asset_ref) => String.t(),
          required(:status) => atom(),
          required(:stage) => non_neg_integer() | nil,
          required(:attempt_number) => pos_integer() | nil,
          required(:execution_pool) => String.t() | nil,
          required(:queue_reason) => String.t() | nil,
          required(:started_at) => DateTime.t() | nil,
          required(:finished_at) => DateTime.t() | nil,
          required(:duration_ms) => non_neg_integer() | nil,
          required(:error) => term(),
          required(:output_metadata) => map() | nil,
          required(:window_identity) => String.t(),
          required(:window) => WindowSummary.t() | nil
        }

  @doc "Returns a compact projection for a step event, or `:ignore` for run events."
  @spec from_event(map()) :: {:ok, t()} | :ignore | {:error, term()}
  def from_event(event) when is_map(event) do
    event_type = event |> field(:event_type) |> to_string()

    case Map.fetch(@step_statuses, event_type) do
      {:ok, status} -> project_step(event, status)
      :error -> :ignore
    end
  end

  def from_event(_event), do: {:error, :invalid_run_event}

  defp project_step(event, status) do
    data = field(event, :data, %{})
    node_result = field(data, :node_result, %{})
    asset_step_id = field(data, :asset_step_id) || field(node_result, :asset_step_id)
    asset_ref = ref_text(field(event, :asset_ref) || field(data, :asset_ref))

    with true <- present?(asset_step_id),
         true <- present?(asset_ref) do
      window = normalize_window(field(node_result, :window) || field(data, :window))
      occurred_at = datetime(field(event, :occurred_at))
      started_at = datetime(field(node_result, :started_at))
      finished_at = datetime(field(node_result, :finished_at))

      {:ok,
       %{
         asset_step_id: asset_step_id,
         asset_ref: asset_ref,
         status: status,
         stage:
           integer(field(node_result, :stage) || field(event, :stage) || field(data, :stage)),
         attempt_number:
           positive_integer(
             field(node_result, :attempt_count) || field(data, :attempt) ||
               field(data, :attempt_count)
           ),
         execution_pool:
           scalar(field(node_result, :execution_pool) || field(data, :execution_pool)),
         queue_reason: scalar(field(data, :queue_reason)),
         started_at: started_at || if(status == :running, do: occurred_at),
         finished_at: finished_at || if(terminal?(status), do: occurred_at),
         duration_ms: integer(field(node_result, :duration_ms)),
         error: field(node_result, :error) || field(data, :error),
         output_metadata: map(field(node_result, :meta)),
         window_identity: window_identity(window),
         window: window
       }}
    else
      false -> :ignore
    end
  end

  defp normalize_window(nil), do: nil

  defp normalize_window(window) when is_map(window) do
    normalized = %{
      key: nil,
      label: field(window, :label),
      kind: window |> field(:kind) |> kind(),
      start_at: window |> field(:start_at) |> datetime(),
      end_at: window |> field(:end_at) |> datetime(),
      timezone: field(window, :timezone)
    }

    source_key = field(window, :key)

    if WindowSummary.empty?(normalized) and not present?(source_key) do
      nil
    else
      identity_fields = Map.drop(normalized, [:key, :label])

      normalized
      |> Map.put(:key, runtime_window_key(source_key, identity_fields))
      |> WindowSummary.public()
    end
  end

  defp normalize_window(_window), do: nil

  defp runtime_window_key(source_key, normalized),
    do: "runtime:" <> digest({source_key, normalized})

  defp window_identity(nil), do: "none"
  defp window_identity(%{key: key}) when is_binary(key), do: key
  defp window_identity(window), do: "runtime:" <> digest(window)

  defp digest(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp ref_text({module, name}) when is_atom(module) and is_atom(name),
    do: Atom.to_string(module) <> ":" <> Atom.to_string(name)

  defp ref_text(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name),
       do: module <> ":" <> name

  defp ref_text(%{module: module, name: name}) when is_binary(module) and is_binary(name),
    do: module <> ":" <> name

  defp ref_text(value) when is_binary(value), do: value
  defp ref_text(_value), do: nil

  defp terminal?(status),
    do: status in [:ok, :error, :timed_out, :cancelled, :skipped_fresh, :blocked]

  defp kind(value) when value in [:hour, :day, :month, :year], do: value
  defp kind("hour"), do: :hour
  defp kind("day"), do: :day
  defp kind("month"), do: :month
  defp kind("year"), do: :year
  defp kind(_value), do: nil

  defp datetime(%DateTime{} = value), do: value

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, parsed, _offset} -> parsed
      _error -> nil
    end
  end

  defp datetime(_value), do: nil

  defp positive_integer(value) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value), do: nil

  defp integer(value) when is_integer(value) and value >= 0, do: value
  defp integer(_value), do: nil

  defp scalar(value) when is_atom(value), do: Atom.to_string(value)
  defp scalar(value) when is_binary(value), do: value
  defp scalar(_value), do: nil

  defp map(value) when is_map(value), do: value
  defp map(_value), do: nil

  defp present?(value), do: is_binary(value) and value != ""

  defp field(value, key, default \\ nil)

  defp field(value, key, default) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  defp field(_value, _key, default), do: default
end

defmodule FavnOrchestrator.API.CommandErrors do
  @moduledoc """
  Maps operator command failures to stable private API responses.

  Internal values are logged only where they aid diagnosis; returned tuples do
  not expose storage or execution details.
  """

  require Logger

  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.Redaction

  @type command_error :: {:error, pos_integer(), String.t(), String.t(), map()}

  @operator_fields %{
    invalid_operator_dependency_mode: {"dependencies", "Invalid dependency mode"},
    invalid_operator_refresh_mode: {"refresh", "Invalid refresh mode"},
    invalid_operator_retry_policy: {"retry_policy", "Invalid retry_policy"},
    invalid_operator_timeout_ms: {"timeout_ms", "Invalid timeout_ms"},
    invalid_operator_coverage_baseline_id:
      {"coverage_baseline_id", "Invalid coverage_baseline_id"},
    invalid_operator_run_context_id: {"run_context_id", "Invalid run_context_id"},
    invalid_operator_metadata: {"metadata", "Invalid metadata"}
  }

  @admission_messages %{
    rebuild_required: "Target requires a rebuild before this run can start",
    target_drift: "Target physical state differs from its recorded generation",
    operator_decision_required:
      "Target requires an explicit operator decision before this run can start"
  }

  @admission_detail_fields [
    :target_id,
    :selected_target_id,
    :blocked_path,
    :blocked_path_target_count,
    :blocked_path_truncated,
    :compatibility_status,
    :reason_code
  ]

  @doc "Maps an operator field validation failure, or returns `nil` when unknown."
  @spec operator(term()) :: command_error() | nil
  def operator({:refresh_include_upstream_requires_dependencies, :all}) do
    {:error, 422, "validation_failed", "force_selected_upstream requires dependencies=all",
     %{fields: ["dependencies", "refresh"]}}
  end

  def operator({:unsupported_retry_option, field, :use_retry_policy})
      when field in [:max_attempts, :retry_backoff_ms] do
    {:error, 422, "validation_failed", "Unsupported retry option; use retry_policy",
     %{
       field: Atom.to_string(field),
       replacement: "retry_policy"
     }}
  end

  def operator({:asset_run_context_timezone_mismatch, expected, actual}) do
    {:error, 422, "validation_failed", "Run context timezone does not match the selection",
     %{field: "run_context_id", expected_timezone: expected, actual_timezone: actual}}
  end

  def operator({:asset_run_context_window_kind_mismatch, expected, actual}) do
    {:error, 422, "validation_failed", "Run context window kind does not match the selection",
     %{field: "run_context_id", expected_kind: name(expected), actual_kind: name(actual)}}
  end

  def operator({reason, _value}) do
    case Map.fetch(@operator_fields, reason) do
      {:ok, {field, message}} ->
        {:error, 422, "validation_failed", message, %{field: field}}

      :error ->
        nil
    end
  end

  def operator(_reason), do: nil

  @doc "Maps a target-admission failure, or returns `nil` when unknown."
  @spec admission(term()) :: command_error() | nil
  def admission({code, details}) when is_map(details) do
    case Map.fetch(@admission_messages, code) do
      {:ok, message} ->
        {:error, 409, Atom.to_string(code), message, Map.take(details, @admission_detail_fields)}

      :error ->
        nil
    end
  end

  def admission(_reason), do: nil

  @doc "Maps a run-window policy failure to an idempotent command result."
  @spec window(term()) :: command_error()
  def window(reason) do
    case window_details(reason) do
      {:ok, message, details} -> {:error, 422, "validation_failed", message, details}
      :error -> {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  @doc "Maps a backfill-range failure to an idempotent command result."
  @spec backfill(term()) :: command_error()
  def backfill(reason) do
    case backfill_details(reason) do
      {:ok, message, details} -> {:error, 422, "validation_failed", message, details}
      :error -> {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  @doc "Sends a backfill-range failure outside an idempotent command callback."
  @spec send_backfill(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def send_backfill(conn, reason) do
    case backfill_details(reason) do
      {:ok, message, details} ->
        Response.error(conn, 422, "validation_failed", message, details)

      :error ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  defp window_details({:missing_window_request, kind}) do
    {:ok, "Pipeline requires an explicit #{kind} window", %{kind: name(kind)}}
  end

  defp window_details({:full_load_not_allowed, kind}) do
    {:ok, "Pipeline does not allow full-load submissions for #{kind} windows",
     %{kind: name(kind)}}
  end

  defp window_details({:window_kind_mismatch, expected, actual}) do
    {:ok, "Window kind #{actual} does not match pipeline policy #{expected}",
     %{expected: name(expected), actual: name(actual)}}
  end

  defp window_details({:window_request_without_policy, kind}) do
    {:ok, "Window request #{kind} was provided for a pipeline without a window policy",
     %{kind: name(kind)}}
  end

  defp window_details({:invalid_window_request, reason}) do
    log_invalid("invalid window request", reason)
    {:ok, "Invalid window request", %{reason: "invalid_window_request"}}
  end

  defp window_details({:invalid_window_value, kind, value}) do
    {:ok, "Invalid #{kind} window value", %{kind: name(kind), value: value}}
  end

  defp window_details({:invalid_timezone, timezone}) do
    {:ok, "Invalid window timezone", %{timezone: timezone}}
  end

  defp window_details({:invalid_positive_integer_option, option}) do
    {:ok, "Invalid positive integer option", %{option: Atom.to_string(option)}}
  end

  defp window_details(_reason), do: :error

  defp backfill_details({:invalid_backfill_range_request, value}) do
    log_invalid("invalid backfill range request", value)
    {:ok, "Invalid backfill range request", %{reason: "invalid_backfill_range_request"}}
  end

  defp backfill_details({:missing_backfill_reference, _opts}) do
    {:ok, "Backfill range request is missing a relative reference", %{}}
  end

  defp backfill_details({:invalid_last_request, value}) do
    log_invalid("invalid relative backfill range", value)
    {:ok, "Invalid relative backfill range", %{reason: "invalid_last_request"}}
  end

  defp backfill_details({:invalid_window_policy_kind, kind}) do
    log_invalid("invalid backfill window policy kind", kind)
    {:ok, "Invalid backfill window kind", %{reason: "invalid_window_policy_kind"}}
  end

  defp backfill_details({:invalid_window_value, kind, value}) do
    {:ok, "Invalid #{kind} window value", %{kind: name(kind), value: value}}
  end

  defp backfill_details({:invalid_timezone, timezone}) do
    {:ok, "Invalid backfill timezone", %{timezone: timezone}}
  end

  defp backfill_details({:too_many_backfill_windows, requested, max}) do
    {:ok, "Backfill range exceeds maximum window count", %{requested: requested, max: max}}
  end

  defp backfill_details({:unsupported_backfill_option, option}) do
    {:ok, "Unsupported backfill option", %{option: Atom.to_string(option)}}
  end

  defp backfill_details({:invalid_positive_integer_option, option}) do
    {:ok, "Invalid positive integer option", %{option: Atom.to_string(option)}}
  end

  defp backfill_details({:coverage_baseline_not_found, baseline_id}) do
    {:ok, "Coverage baseline was not found", %{coverage_baseline_id: baseline_id}}
  end

  defp backfill_details({:coverage_baseline_pipeline_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline pipeline does not match requested pipeline",
     %{baseline_pipeline: name(baseline), requested_pipeline: name(requested)}}
  end

  defp backfill_details({:coverage_baseline_not_ok, status}) do
    {:ok, "Coverage baseline is not eligible for reuse", %{status: name(status)}}
  end

  defp backfill_details({:coverage_baseline_window_kind_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline window kind does not match requested range kind",
     %{baseline_window_kind: name(baseline), requested_window_kind: name(requested)}}
  end

  defp backfill_details({:coverage_baseline_timezone_mismatch, baseline, requested}) do
    {:ok, "Coverage baseline timezone does not match requested range timezone",
     %{baseline_timezone: baseline, requested_timezone: requested}}
  end

  defp backfill_details(_reason), do: :error

  defp log_invalid(message, value) do
    diagnostic = Redaction.redact_operational_bounded(%{reason: value})
    Logger.error("#{message}: #{inspect(diagnostic)}")
  end

  defp name(nil), do: nil
  defp name(value) when is_atom(value), do: Atom.to_string(value)
  defp name(value), do: DTO.normalize(value)
end

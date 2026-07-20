defmodule FavnOrchestrator.RunServer.Execution.ResultSanitizer do
  @moduledoc """
  Normalizes runner-owned results before the orchestrator persists them.

  Runner clients are expected to return `Favn.Contracts.RunnerError` values,
  but this boundary also handles older or faulty clients that return arbitrary
  terms. Operational error text is redacted and bounded before it can enter run
  snapshots or events.
  """

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.ResourceOutcome
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Redaction

  @max_error_bytes 8_192

  @doc """
  Sanitizes a runner result and all nested asset and attempt errors.
  """
  @spec sanitize(RunnerResult.t()) :: RunnerResult.t()
  def sanitize(%RunnerResult{} = result) do
    %{
      result
      | error: sanitize_error(result.error),
        asset_results: sanitize_asset_results(result.asset_results),
        resource_outcomes: sanitize_resource_outcomes(result.resource_outcomes)
    }
  end

  @doc """
  Sanitizes a list of runner-owned asset results.

  Invalid list values are treated as an empty result set so a faulty runner
  cannot make result persistence raise.
  """
  @spec sanitize_asset_results(term()) :: list()
  def sanitize_asset_results(results) when is_list(results),
    do: Enum.map(results, &sanitize_asset_result/1)

  def sanitize_asset_results(_results), do: []

  @doc """
  Nests non-empty runner metadata under the orchestrator run metadata.

  The run metadata always wins ownership of its top-level keys.
  """
  @spec merge_metadata(map(), term()) :: map()
  def merge_metadata(run_metadata, runner_metadata)
      when is_map(run_metadata) and is_map(runner_metadata) do
    if map_size(runner_metadata) == 0 do
      run_metadata
    else
      Map.put(run_metadata, :runner_metadata, runner_metadata)
    end
  end

  def merge_metadata(run_metadata, _runner_metadata) when is_map(run_metadata),
    do: run_metadata

  defp sanitize_resource_outcomes(outcomes) do
    case ResourceOutcome.normalize_many(outcomes) do
      {:ok, normalized} -> normalized
      {:error, _reason} -> []
    end
  end

  defp sanitize_asset_result(%RunnerAssetResult{} = result) do
    %{result | error: sanitize_error(result.error), attempts: sanitize_attempts(result.attempts)}
  end

  defp sanitize_asset_result(%AssetResult{} = result) do
    %{result | error: sanitize_error(result.error), attempts: sanitize_attempts(result.attempts)}
  end

  defp sanitize_asset_result(result), do: result

  defp sanitize_attempts(attempts) when is_list(attempts) do
    Enum.map(attempts, fn
      %{error: error} = attempt -> %{attempt | error: sanitize_error(error)}
      %{"error" => error} = attempt -> %{attempt | "error" => sanitize_error(error)}
      attempt -> attempt
    end)
  end

  defp sanitize_attempts(_attempts), do: []

  defp sanitize_error(nil), do: nil

  defp sanitize_error(%RunnerError{} = error) do
    sanitized =
      error
      |> Map.from_struct()
      |> Map.put(:redacted?, true)
      |> RunnerError.new()

    %{
      sanitized
      | message: safe_error_message(sanitized.message),
        reason: sanitize_runner_error_reason(sanitized.reason),
        details: bound_value(sanitized.details)
    }
  end

  defp sanitize_error(
         %{"kind" => _kind, "message" => _message, "reason" => _reason, "type" => _type} =
           error
       ) do
    %{
      "kind" => string_value(Map.fetch!(error, "kind")),
      "message" => safe_error_message(Map.fetch!(error, "message")),
      "reason" => safe_error_reason(Map.fetch!(error, "reason")),
      "type" => string_value(Map.fetch!(error, "type"))
    }
  end

  defp sanitize_error(%{kind: kind} = error) do
    reason = Map.get(error, :reason)
    message = Map.get(error, :message) || error_message(reason) || reason || "Runner error"

    %{
      "kind" => string_value(kind),
      "message" => safe_error_message(message),
      "reason" => safe_error_reason(reason),
      "type" => error_type(reason)
    }
  end

  defp sanitize_error(%{type: _type} = error), do: sanitize_structured_error(error)
  defp sanitize_error(%{"type" => _type} = error), do: sanitize_structured_error(error)

  defp sanitize_error(error) do
    %{
      "kind" => "error",
      "message" => safe_error_message(error_message(error) || error),
      "reason" => safe_error_reason(error),
      "type" => error_type(error)
    }
  end

  defp safe_error_message(value) do
    case redact_error_field(:message, value) do
      nil -> "Runner error"
      redacted -> string_value(redacted)
    end
  end

  defp safe_error_reason(value), do: redact_error_field(:reason, value) |> inspect_value()

  defp sanitize_runner_error_reason(nil), do: nil

  defp sanitize_runner_error_reason(reason) do
    case redact_error_field(:reason, reason) do
      redacted when is_binary(redacted) -> truncate_string(redacted)
      redacted -> inspect_value(redacted)
    end
  end

  defp redact_error_field(key, value) when is_atom(key) do
    case Redaction.redact_operational(%{key => value}) do
      %{^key => redacted} -> redacted
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp sanitize_structured_error(%{type: :missing_runtime_config} = error),
    do: sanitize_runtime_config_diagnostic(error)

  defp sanitize_structured_error(%{"type" => "missing_runtime_config"} = error),
    do: sanitize_runtime_config_diagnostic(error)

  defp sanitize_structured_error(error) when is_map(error) do
    error
    |> Map.drop([:stacktrace, "stacktrace"])
    |> Map.new(fn {key, value} -> {key, sanitize_structured_error_value(key, value)} end)
  end

  defp sanitize_runtime_config_diagnostic(error) when is_map(error) do
    error
    |> Map.drop([:stacktrace, "stacktrace"])
    |> Map.new(fn {key, value} -> {key, sanitize_runtime_config_diagnostic_value(key, value)} end)
  end

  defp sanitize_runtime_config_diagnostic_value(key, value) when key in [:message, "message"],
    do: string_value(value)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_map(value),
    do: sanitize_runtime_config_diagnostic(value)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_list(value),
    do: Enum.map(value, &sanitize_runtime_config_diagnostic_nested/1)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_runtime_config_diagnostic_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_runtime_config_diagnostic_value(_key, value), do: value

  defp sanitize_runtime_config_diagnostic_nested(value) when is_map(value),
    do: sanitize_runtime_config_diagnostic(value)

  defp sanitize_runtime_config_diagnostic_nested(value) when is_list(value),
    do: Enum.map(value, &sanitize_runtime_config_diagnostic_nested/1)

  defp sanitize_runtime_config_diagnostic_nested(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_runtime_config_diagnostic_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_runtime_config_diagnostic_nested(value), do: value

  defp sanitize_structured_error_value(key, value) do
    cond do
      operational_error_key?(key) ->
        key
        |> normalize_error_key()
        |> redact_error_field(value)
        |> bound_value()

      is_map(value) ->
        sanitize_structured_error(value)

      is_list(value) ->
        Enum.map(value, &sanitize_structured_error_nested/1)

      is_tuple(value) ->
        value
        |> Tuple.to_list()
        |> Enum.map(&sanitize_structured_error_nested/1)
        |> List.to_tuple()

      true ->
        value
    end
  end

  defp sanitize_structured_error_nested(value) when is_map(value),
    do: sanitize_structured_error(value)

  defp sanitize_structured_error_nested(value) when is_list(value),
    do: Enum.map(value, &sanitize_structured_error_nested/1)

  defp sanitize_structured_error_nested(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_structured_error_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_structured_error_nested(value), do: value

  defp operational_error_key?(key) when key in [:message, :reason, :error, :exception], do: true

  defp operational_error_key?(key) when key in ["message", "reason", "error", "exception"],
    do: true

  defp operational_error_key?(_key), do: false

  defp normalize_error_key(key) when is_atom(key), do: key
  defp normalize_error_key("message"), do: :message
  defp normalize_error_key("reason"), do: :reason
  defp normalize_error_key("error"), do: :error
  defp normalize_error_key("exception"), do: :exception

  defp error_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp error_message(_error), do: nil

  defp error_type(%{__exception__: true, __struct__: module}), do: Atom.to_string(module)
  defp error_type(error) when is_boolean(error), do: "boolean"
  defp error_type(error) when is_atom(error), do: Atom.to_string(error)
  defp error_type(error), do: error |> term_type() |> Atom.to_string()

  defp term_type(term) when is_map(term), do: :map
  defp term_type(term) when is_tuple(term), do: :tuple
  defp term_type(term) when is_list(term), do: :list
  defp term_type(term) when is_binary(term), do: :string
  defp term_type(term) when is_number(term), do: :number
  defp term_type(_term), do: :term

  defp string_value(value) when is_binary(value), do: truncate_string(value)
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value), do: inspect_value(value)

  defp inspect_value(value) do
    value
    |> inspect(limit: 20, printable_limit: 4_096)
    |> truncate_string()
  rescue
    _error -> "#Inspect.Error<>"
  end

  defp truncate_string(value) when is_binary(value) do
    if byte_size(value) > @max_error_bytes do
      String.slice(value, 0, @max_error_bytes) <> "..."
    else
      value
    end
  end

  defp bound_value(value) when is_binary(value), do: truncate_string(value)
  defp bound_value(value) when is_map(value), do: Map.new(value, &bound_entry/1)
  defp bound_value(value) when is_list(value), do: Enum.map(value, &bound_value/1)

  defp bound_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&bound_value/1)
    |> List.to_tuple()
  end

  defp bound_value(value), do: value

  defp bound_entry({key, value}), do: {key, bound_value(value)}
end

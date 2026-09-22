defmodule FavnOrchestrator.Storage.JsonSafe do
  @moduledoc false

  alias Favn.Contracts.RunnerAssetEvidence
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Run.AssetResult
  alias Favn.SQL.{Check, CheckResult, ContractValidation}
  alias Favn.Window.Selection
  alias FavnOrchestrator.Redaction

  @key_collision "[DIAGNOSTIC KEY COLLISION]"
  @max_depth 8
  @max_entries 50
  @max_string_bytes 8_192
  @max_check_metrics 32
  @max_sql_check_results Check.max_per_asset() + Check.max_contract_per_asset()
  @max_contract_columns ContractValidation.max_observed_columns()
  @max_contract_differences @max_contract_columns * 3 + 2

  @sensitive_key_fragments ~w(
    token tokens password secret authorization cookie credential credentials database dsn url uri
    api_key apikey access_key accesskey private_key privatekey
  )

  @spec data(term()) :: map() | list() | String.t() | number() | boolean() | nil
  def data(value), do: data(value, nil, @max_depth)

  @spec output_metadata(term()) :: map() | list() | String.t() | number() | boolean() | nil
  def output_metadata(value), do: data(value)

  @spec execution_evidence(term()) :: map() | nil
  def execution_evidence(%RunnerAssetEvidence{} = value),
    do: value |> Map.from_struct() |> execution_evidence()

  def execution_evidence(value) when is_map(value) do
    ordinary =
      value
      |> Enum.reject(fn {key, _value} ->
        key_to_string(key) in ["check_results", "contract_validation"]
      end)
      |> Enum.take(@max_entries)
      |> diagnostic_map(@max_depth - 1)

    ordinary
    |> maybe_put_assurance_field(value, :check_results, &check_results_to_dto/1)
    |> maybe_put_assurance_field(value, :contract_validation, &contract_validation_to_dto/1)
  end

  def execution_evidence(_value), do: nil

  @doc false
  @spec window_selection(Selection.t() | nil) :: map() | nil
  def window_selection(nil), do: nil

  def window_selection(%Selection{} = selection) do
    %{
      "intent" => Atom.to_string(selection.intent),
      "requested_anchors" => Enum.map(selection.requested_anchors, &selection_anchor/1),
      "expansion" => selection_expansion(selection.expansion),
      "effective_anchors" => Enum.map(selection.effective_anchors, &selection_anchor/1),
      "timezone" => selection.timezone
    }
  end

  @spec error(term()) :: map() | nil
  def error(nil), do: nil

  def error(%RunnerError{} = value) do
    %{
      "kind" => scalar_string(value.kind, "error"),
      "type" => scalar_string(value.type, "term"),
      "phase" => scalar_string(value.phase, nil),
      "message" => safe_error_message(value.message),
      "reason" => safe_existing_error_reason(value.reason),
      "details" => data(value.details, "details", @max_depth - 1),
      "retryable" => value.retryable?,
      "outcome" => scalar_string(value.outcome, nil),
      "redacted" => true,
      "truncated" => false
    }
    |> Enum.reject(fn {_key, child_value} -> is_nil(child_value) end)
    |> Map.new()
  end

  def error(%{type: :missing_runtime_config} = value), do: runtime_config_diagnostic(value)
  def error(%{"type" => "missing_runtime_config"} = value), do: runtime_config_diagnostic(value)

  def error(%{"kind" => kind, "message" => message, "type" => type} = value) do
    %{
      "kind" => scalar_string(kind, "error"),
      "type" => meaningful_error_type(type, value, kind),
      "message" => safe_error_message(message),
      "reason" => safe_existing_error_reason(Map.get(value, "reason")),
      "redacted" => true,
      "truncated" => false
    }
    |> maybe_put_error_details(Map.get(value, "details"))
    |> Map.merge(data(Map.take(value, ["phase", "outcome", "retryable"])))
  end

  def error(%{kind: kind} = value) do
    reason = Map.get(value, :reason) || Map.get(value, "reason")
    message = Map.get(value, :message) || Map.get(value, "message") || exception_message(reason)

    %{
      "kind" => scalar_string(kind, "error"),
      "type" => meaningful_error_type(Map.get(value, :type), value, kind),
      "message" => safe_error_message(message || reason || value),
      "reason" => safe_error_reason(reason || value),
      "redacted" => true,
      "truncated" => false
    }
    |> maybe_put_error_details(Map.get(value, :details) || Map.get(value, "details"))
  end

  # Orchestrator error maps name their class in `type`. Keeping that class and
  # any explicit message lets operator reads show a stable code without
  # inspecting the whole map; the reason and remaining fields stay bounded and
  # separate so they are never rendered as the message.
  def error(%{type: type} = value)
      when is_atom(type) and not is_nil(type) and not is_boolean(type) and
             not is_map_key(value, :__struct__) do
    message = Map.get(value, :message)
    reason = Map.get(value, :reason)
    extra = Map.drop(value, [:type, :message, :reason, :details])

    details =
      case Map.get(value, :details) do
        explicit when is_map(explicit) -> Map.merge(extra, explicit)
        _none -> extra
      end

    # Every clause emits the same four keys so a stored error re-encodes to
    # itself on the next snapshot write instead of falling to the catch-all.
    %{
      "kind" => "error",
      "type" => scalar_string(type, "error"),
      "message" => safe_error_message(message || type),
      "reason" => safe_existing_error_reason(reason || Atom.to_string(type)),
      "redacted" => true,
      "truncated" => false
    }
    |> maybe_put_error_details(if(map_size(details) == 0, do: nil, else: details))
  end

  def error(%{__exception__: true, __struct__: module} = exception) when is_atom(module) do
    %{
      "kind" => "error",
      "type" => scalar_string(module, "error"),
      "message" => safe_error_message(exception_message(exception) || exception),
      "reason" => safe_error_reason(exception),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(%{__exception__: true} = exception) do
    message = Map.get(exception, :message) || Map.get(exception, "message") || exception

    %{
      "kind" => "error",
      "type" => error_type(exception),
      "message" => safe_error_message(message),
      "reason" => safe_error_reason(exception),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(value) do
    %{
      "kind" => "error",
      "type" => error_type(value),
      "message" => safe_error_message(value),
      "reason" => safe_error_reason(value),
      "redacted" => true,
      "truncated" => false
    }
  end

  @spec ref(Favn.Ref.t() | term()) :: map() | nil
  def ref({module, name}) when is_atom(module) and is_atom(name) do
    %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}
  end

  def ref(_value), do: nil

  defp data(%CheckResult{} = value, _key, _depth), do: check_result_to_dto(value)

  defp data(%ContractValidation{} = value, _key, _depth),
    do: contract_validation_to_dto(value)

  defp data(%Selection{} = value, _key, _depth), do: window_selection(value)

  defp data(_value, _key, depth) when depth <= 0, do: "[TRUNCATED]"
  defp data(%Decimal{} = value, _key, _depth), do: Decimal.to_string(value)
  defp data(%Date{} = value, _key, _depth), do: Date.to_iso8601(value)
  defp data(%DateTime{} = value, _key, _depth), do: DateTime.to_iso8601(value)
  defp data(%NaiveDateTime{} = value, _key, _depth), do: NaiveDateTime.to_iso8601(value)
  defp data(%Time{} = value, _key, _depth), do: Time.to_iso8601(value)
  defp data(%RunnerAssetEvidence{} = value, _key, _depth), do: execution_evidence(value)

  defp data(%RunnerAssetResult{} = value, _key, depth), do: runner_asset_result(value, depth)
  defp data(%AssetResult{} = value, _key, depth), do: asset_result(value, depth)

  defp data(%{__exception__: true} = value, _key, _depth), do: error(value)

  defp data(%_{} = value, key, depth) do
    value
    |> Map.from_struct()
    |> data(key, depth - 1)
  end

  defp data(value, _key, depth) when is_map(value) do
    value
    |> Enum.take(@max_entries)
    |> diagnostic_map(depth - 1)
  end

  defp data(value, _key, depth) when is_list(value) do
    value
    |> Enum.take(@max_entries)
    |> Enum.map(&data(&1, nil, depth - 1))
  end

  defp data({module, name}, _key, _depth) when is_atom(module) and is_atom(name),
    do: diagnostic_map(ref({module, name}), 1)

  defp data(value, _key, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.take(@max_entries)
    |> Enum.map(&data(&1, nil, depth - 1))
  end

  defp data(value, _key, _depth) when is_binary(value), do: truncate(value)
  defp data(value, _key, _depth) when is_integer(value) or is_float(value), do: value
  defp data(value, _key, _depth) when is_boolean(value), do: value
  defp data(nil, _key, _depth), do: nil
  defp data(value, _key, _depth) when is_atom(value), do: value |> Atom.to_string() |> truncate()
  defp data(value, _key, _depth), do: inspect_value(value)

  defp selection_expansion(:none), do: "none"
  defp selection_expansion({:lookback, count}), do: ["lookback", count]

  defp selection_anchor(anchor) do
    %{
      "kind" => Atom.to_string(anchor.kind),
      "start_at" => DateTime.to_iso8601(anchor.start_at),
      "end_at" => DateTime.to_iso8601(anchor.end_at),
      "timezone" => anchor.timezone
    }
  end

  defp asset_result(%AssetResult{} = result, depth) do
    %{
      "ref" => ref(result.ref),
      "stage" => result.stage,
      "status" => atom_string(result.status),
      "started_at" => data(result.started_at, nil, depth - 1),
      "finished_at" => data(result.finished_at, nil, depth - 1),
      "duration_ms" => result.duration_ms,
      "meta" => output_metadata(result.meta),
      "evidence" => execution_evidence(result.evidence),
      "error" => error(result.error),
      "attempt_count" => result.attempt_count,
      "max_attempts" => result.max_attempts,
      "attempts" => result.attempts |> List.wrap() |> bounded_attempts(depth),
      "next_retry_at" => data(result.next_retry_at, nil, depth - 1)
    }
  end

  defp runner_asset_result(%RunnerAssetResult{} = result, depth) do
    %{
      "ref" => ref(result.ref),
      "status" => atom_string(result.status),
      "started_at" => data(result.started_at, nil, depth - 1),
      "finished_at" => data(result.finished_at, nil, depth - 1),
      "duration_ms" => result.duration_ms,
      "meta" => output_metadata(result.meta),
      "evidence" => execution_evidence(result.evidence),
      "error" => error(result.error),
      "attempt_count" => result.attempt_count,
      "max_attempts" => result.max_attempts,
      "attempts" => result.attempts |> List.wrap() |> bounded_attempts(depth),
      "asset_step_id" => result.asset_step_id
    }
    |> Enum.reject(fn {_key, child_value} -> is_nil(child_value) end)
    |> Map.new()
  end

  defp attempt(%{} = attempt, depth) do
    attempt
    |> data(nil, depth)
    |> Map.update("error", nil, &error/1)
  end

  defp attempt(value, depth), do: data(value, nil, depth)

  defp bounded_attempts(attempts, depth) do
    attempts
    |> Enum.take(@max_entries)
    |> Enum.map(&attempt(&1, depth - 1))
  end

  defp maybe_put_assurance_field(dto, source, key, mapper) do
    if has_field?(source, key) do
      Map.put(dto, Atom.to_string(key), mapper.(field(source, key)))
    else
      dto
    end
  end

  defp check_results_to_dto(results) when is_list(results) do
    results
    |> Enum.take(@max_sql_check_results)
    |> Enum.map(&check_result_to_dto/1)
  end

  defp check_results_to_dto(value), do: data(value)

  defp check_result_to_dto(value) when is_map(value) do
    %{
      "name" => scalar_string(field(value, :name), nil),
      "phase" => scalar_string(field(value, :phase), nil),
      "outcome" => scalar_string(field(value, :outcome), nil),
      "origin" => scalar_string(field(value, :origin, :authored), "authored"),
      "claim_id" => scalar_string(field(value, :claim_id), nil),
      "message" => scalar_string(field(value, :message), nil),
      "duration_ms" => data(field(value, :duration_ms), nil, @max_depth),
      "reason" => data(field(value, :reason), nil, @max_depth),
      "metrics" => check_metrics_to_dto(field(value, :metrics, %{}))
    }
    |> Enum.reject(fn {_key, child} -> is_nil(child) end)
    |> Map.new()
  end

  defp check_result_to_dto(value), do: data(value)

  defp check_metrics_to_dto(metrics) when is_map(metrics) do
    metrics
    |> Enum.take(@max_check_metrics)
    |> diagnostic_map(@max_depth)
  end

  defp check_metrics_to_dto(_metrics), do: %{}

  defp contract_validation_to_dto(value) when is_map(value) do
    %{
      "status" => scalar_string(field(value, :status), nil),
      "expected_columns" =>
        bounded_assurance_list(field(value, :expected_columns, []), @max_contract_columns),
      "observed_columns" =>
        bounded_assurance_list(field(value, :observed_columns, []), @max_contract_columns),
      "differences" =>
        bounded_assurance_list(field(value, :differences, []), @max_contract_differences),
      "observed_column_count" => data(field(value, :observed_column_count), nil, @max_depth),
      "observed_truncated?" => data(field(value, :observed_truncated?, false), nil, @max_depth)
    }
    |> Enum.reject(fn {_key, child} -> is_nil(child) end)
    |> Map.new()
  end

  defp contract_validation_to_dto(value), do: data(value)

  defp bounded_assurance_list(values, limit) when is_list(values) do
    values
    |> Enum.take(limit)
    |> Enum.map(&data(&1, nil, @max_depth))
  end

  defp bounded_assurance_list(_values, _limit), do: []

  defp has_field?(value, key) when is_map(value),
    do: Map.has_key?(value, key) or Map.has_key?(value, Atom.to_string(key))

  defp field(value, key, default \\ nil) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  defp runtime_config_diagnostic(value) when is_map(value) do
    value
    |> Map.drop([:stacktrace, "stacktrace"])
    |> data(nil, @max_depth)
  end

  defp safe_error_message(value) do
    case Redaction.redact_operational(%{message: value}) do
      %{message: redacted} -> scalar_string(redacted, "Runner error")
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp safe_error_reason(value) do
    case Redaction.redact_operational(%{reason: value}) do
      %{reason: redacted} -> inspect_value(redacted)
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp safe_existing_error_reason(value) when is_binary(value), do: safe_error_message(value)
  defp safe_existing_error_reason(value), do: safe_error_reason(value)

  defp maybe_put_error_details(error, details) when is_map(details),
    do: Map.put(error, "details", data(details, "details", @max_depth - 1))

  defp maybe_put_error_details(error, _details), do: error

  defp exception_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp exception_message(_value), do: nil

  defp meaningful_error_type(type, value, kind) do
    details =
      case Map.get(value, :details) || Map.get(value, "details") do
        details when is_map(details) -> details
        _ -> %{}
      end

    [
      Map.get(details, :reason_code),
      Map.get(details, "reason_code"),
      type,
      if(Map.get(value, :reason) != nil, do: error_type(value.reason)),
      kind
    ]
    |> Enum.map(fn candidate ->
      if is_binary(candidate), do: String.trim(candidate), else: candidate
    end)
    |> Enum.find(fn candidate -> candidate not in [nil, "", "nil", "null", false, true] end)
    |> scalar_string("error")
  end

  defp error_type(%{__struct__: module}) when is_atom(module), do: scalar_string(module, "error")
  defp error_type(value) when is_boolean(value), do: "boolean"
  defp error_type(value) when is_atom(value), do: scalar_string(value, "error")
  defp error_type(value) when is_map(value), do: "map"
  defp error_type(value) when is_tuple(value), do: "tuple"
  defp error_type(value) when is_list(value), do: "list"
  defp error_type(value) when is_binary(value), do: "string"
  defp error_type(value) when is_number(value), do: "number"
  defp error_type(_value), do: "term"

  defp scalar_string(value, _default) when is_binary(value), do: truncate(value)
  defp scalar_string(nil, default), do: default

  defp scalar_string(value, _default) when is_atom(value),
    do: value |> Atom.to_string() |> truncate()

  defp scalar_string(value, _default), do: inspect_value(value)

  defp atom_string(nil), do: nil
  defp atom_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_string(value) when is_binary(value), do: value
  defp atom_string(value), do: inspect_value(value)

  defp key_to_string(key) when is_binary(key), do: key
  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key), do: inspect_value(key)

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@sensitive_key_fragments, &String.contains?(key, &1))
  end

  defp diagnostic_map(entries, depth) do
    Enum.reduce(entries, %{}, fn {key, value}, acc ->
      original_key = key_to_string(key)
      normalized_key = truncate(original_key)

      normalized_value =
        if sensitive_key?(original_key) or sensitive_key?(normalized_key),
          do: redact_sensitive_value(value),
          else: data(value, normalized_key, depth)

      Map.update(acc, normalized_key, normalized_value, fn _ -> @key_collision end)
    end)
  end

  defp redact_sensitive_value(@key_collision), do: @key_collision
  defp redact_sensitive_value(value) when is_boolean(value), do: value
  defp redact_sensitive_value(nil), do: nil
  defp redact_sensitive_value(_value), do: "[REDACTED]"

  defp truncate(value) when is_binary(value) do
    if String.valid?(value) do
      value |> String.replace(<<0>>, "\\u0000") |> truncate_valid()
    else
      value
      |> inspect(limit: 20, printable_limit: @max_string_bytes)
      |> truncate_valid()
    end
  end

  defp truncate_valid(value) when byte_size(value) <= @max_string_bytes, do: value

  defp truncate_valid(value) do
    suffix = "..."
    content_bytes = @max_string_bytes - byte_size(suffix)
    valid_prefix(value, content_bytes) <> suffix
  end

  defp valid_prefix(_value, size) when size <= 0, do: ""

  defp valid_prefix(value, size) do
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix) do
      prefix
    else
      valid_prefix(value, size - 1)
    end
  end

  defp inspect_value(value) do
    value
    |> inspect(limit: 20, printable_limit: @max_string_bytes)
    |> truncate()
  rescue
    _error -> "#Inspect.Error<>"
  end
end

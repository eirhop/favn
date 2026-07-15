defmodule FavnOrchestrator.Storage.JsonSafe do
  @moduledoc false

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Run.AssetResult
  alias Favn.SQL.{Check, CheckResult, ContractValidation}
  alias FavnOrchestrator.Redaction

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
  def output_metadata(value) when is_map(value) do
    ordinary =
      value
      |> Enum.reject(fn {key, _value} ->
        key_to_string(key) in ["check_results", "contract_validation"]
      end)
      |> Enum.take(@max_entries)
      |> Map.new(fn {key, child} ->
        key_string = key_to_string(key)

        normalized =
          if sensitive_key?(key_string),
            do: redact_sensitive_value(child),
            else: data(child, key_string, @max_depth - 1)

        {key_string, normalized}
      end)

    ordinary
    |> maybe_put_assurance_field(value, :check_results, &check_results_to_dto/1)
    |> maybe_put_assurance_field(value, :contract_validation, &contract_validation_to_dto/1)
  end

  def output_metadata(value), do: data(value)

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
      "redacted" => true,
      "truncated" => false
    }
    |> Enum.reject(fn {_key, child_value} -> is_nil(child_value) end)
    |> Map.new()
  end

  def error(%{type: :missing_runtime_config} = value), do: runtime_config_diagnostic(value)
  def error(%{"type" => "missing_runtime_config"} = value), do: runtime_config_diagnostic(value)

  def error(%{"kind" => kind, "message" => message, "reason" => reason, "type" => type}) do
    %{
      "kind" => scalar_string(kind, "error"),
      "type" => scalar_string(type, "term"),
      "message" => safe_error_message(message),
      "reason" => safe_existing_error_reason(reason),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(%{kind: kind} = value) do
    reason = Map.get(value, :reason) || Map.get(value, "reason")
    message = Map.get(value, :message) || Map.get(value, "message") || exception_message(reason)

    %{
      "kind" => scalar_string(kind, "error"),
      "type" => error_type(reason),
      "message" => safe_error_message(message || reason || value),
      "reason" => safe_error_reason(reason || value),
      "redacted" => true,
      "truncated" => false
    }
  end

  def error(%{__exception__: true, __struct__: module} = exception) when is_atom(module) do
    %{
      "kind" => "error",
      "type" => Atom.to_string(module),
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

  defp data(_value, _key, depth) when depth <= 0, do: "[TRUNCATED]"
  defp data(%Decimal{} = value, _key, _depth), do: Decimal.to_string(value)
  defp data(%Date{} = value, _key, _depth), do: Date.to_iso8601(value)
  defp data(%DateTime{} = value, _key, _depth), do: DateTime.to_iso8601(value)
  defp data(%NaiveDateTime{} = value, _key, _depth), do: NaiveDateTime.to_iso8601(value)
  defp data(%Time{} = value, _key, _depth), do: Time.to_iso8601(value)
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
    |> Map.new(fn {child_key, child_value} ->
      key_string = key_to_string(child_key)

      normalized_value =
        if sensitive_key?(key_string) do
          redact_sensitive_value(child_value)
        else
          data(child_value, key_string, depth - 1)
        end

      {key_string, normalized_value}
    end)
  end

  defp data(value, _key, depth) when is_list(value) do
    value
    |> Enum.take(@max_entries)
    |> Enum.map(&data(&1, nil, depth - 1))
  end

  defp data({module, name}, _key, _depth) when is_atom(module) and is_atom(name),
    do: ref({module, name})

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
  defp data(value, _key, _depth) when is_atom(value), do: Atom.to_string(value)
  defp data(value, _key, _depth), do: inspect_value(value)

  defp asset_result(%AssetResult{} = result, depth) do
    %{
      "ref" => ref(result.ref),
      "stage" => result.stage,
      "status" => atom_string(result.status),
      "started_at" => data(result.started_at, nil, depth - 1),
      "finished_at" => data(result.finished_at, nil, depth - 1),
      "duration_ms" => result.duration_ms,
      "meta" => output_metadata(result.meta),
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
    |> Map.new(fn {key, metric} ->
      key_string = key_to_string(key)

      normalized =
        if sensitive_key?(key_string),
          do: redact_sensitive_value(metric),
          else: data(metric, key_string, @max_depth)

      {key_string, normalized}
    end)
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

  defp exception_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp exception_message(_value), do: nil

  defp error_type(%{__exception__: true, __struct__: module}) when is_atom(module),
    do: Atom.to_string(module)

  defp error_type(%{__struct__: module}) when is_atom(module), do: Atom.to_string(module)
  defp error_type(value) when is_boolean(value), do: "boolean"
  defp error_type(nil), do: "nil"
  defp error_type(value) when is_atom(value), do: Atom.to_string(value)
  defp error_type(value) when is_map(value), do: "map"
  defp error_type(value) when is_tuple(value), do: "tuple"
  defp error_type(value) when is_list(value), do: "list"
  defp error_type(value) when is_binary(value), do: "string"
  defp error_type(value) when is_number(value), do: "number"
  defp error_type(_value), do: "term"

  defp scalar_string(value, _default) when is_binary(value), do: truncate(value)
  defp scalar_string(nil, default), do: default
  defp scalar_string(value, _default) when is_atom(value), do: Atom.to_string(value)
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

  defp redact_sensitive_value(value) when is_boolean(value), do: value
  defp redact_sensitive_value(nil), do: nil
  defp redact_sensitive_value(_value), do: "[REDACTED]"

  defp truncate(value) when is_binary(value) do
    if String.valid?(value) do
      truncate_valid(value)
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

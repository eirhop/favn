defmodule FavnRunner.RuntimeInputResolver do
  @moduledoc false

  alias Favn.Run.Context
  alias Favn.RuntimeInputResolver.Ref
  alias Favn.SQL.CancelToken
  alias Favn.SQL.Error, as: SQLError
  alias Favn.SQL.{ParamBinding, Params, Render, WritePlan}
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Error
  alias Favn.SQLAsset.RuntimeInputs.Error, as: ResolverError
  alias Favn.SQLAsset.RuntimeInputs.Result

  @default_timeout_ms 30_000
  @max_params 128
  @max_param_payload_bytes 4 * 1_024 * 1_024
  @max_identity_bytes 1_024
  @max_metadata_bytes 64 * 1_024
  @max_metadata_entries 128
  @max_error_message_bytes 4_096

  defmodule Resolution do
    @moduledoc false

    @derive {Inspect, except: [:params, :sensitive_values]}
    @enforce_keys [
      :resolver,
      :params,
      :identity,
      :metadata,
      :sensitive_params,
      :sensitive_values,
      :duration_ms
    ]
    defstruct [
      :resolver,
      :params,
      :identity,
      :metadata,
      :sensitive_params,
      :sensitive_values,
      :duration_ms
    ]

    @type t :: %__MODULE__{
            resolver: module(),
            params: map(),
            identity: String.t(),
            metadata: map(),
            sensitive_params: [atom() | String.t()],
            sensitive_values: [term()],
            duration_ms: non_neg_integer()
          }
  end

  @spec resolve(Ref.t(), Context.t(), map(), keyword()) ::
          {:ok, Resolution.t()} | {:error, Error.t()}
  def resolve(%Ref{module: module}, %Context{} = context, submitted_params, opts)
      when is_map(submitted_params) and is_list(opts) do
    started_at = System.monotonic_time(:millisecond)

    result =
      with :ok <- ensure_runtime_module(module),
           {:ok, resolver_result} <- invoke(module, context, resolution_timeout_ms(opts)),
           {:ok, validated} <- validate_resolver_result(resolver_result, module),
           {:ok, merged_params} <- merge_params(submitted_params, validated.params, module) do
        sensitive_values = sensitive_values(validated.params, validated.sensitive_params)

        {:ok,
         %Resolution{
           resolver: module,
           params: merged_params,
           identity: validated.identity,
           metadata: validated.metadata,
           sensitive_params: validated.sensitive_params,
           sensitive_values: sensitive_values,
           duration_ms: 0
         }}
      end

    duration_ms = max(System.monotonic_time(:millisecond) - started_at, 0)
    result = put_duration(result, duration_ms)
    emit_telemetry(module, context.asset.ref, result, duration_ms)
    result
  end

  @spec lineage(Resolution.t()) :: map()
  def lineage(%Resolution{} = resolution) do
    identity = redact_value(resolution.identity, resolution.sensitive_values)

    %{
      resolver: resolution.resolver,
      input_identity: if(identity == :redacted, do: "[REDACTED]", else: identity),
      input_metadata: redact_value(resolution.metadata, resolution.sensitive_values),
      duration_ms: resolution.duration_ms
    }
  end

  @spec redact(term(), Resolution.t()) :: term()
  def redact(%Error{} = error, %Resolution{sensitive_values: values}) do
    %Error{
      error
      | message: redact_value(error.message, values),
        details: redact_value(error.details, values),
        cause: redact_value(error.cause, values)
    }
  end

  def redact(term, %Resolution{sensitive_values: values}) do
    redact_value(term, values)
  end

  @spec redact_render(Render.t(), Resolution.t()) :: Render.t()
  def redact_render(
        %Render{params: %Params{bindings: bindings} = params} = render,
        %Resolution{sensitive_params: sensitive_params}
      ) do
    names = MapSet.new(sensitive_params, &normalize_param_name/1)

    bindings =
      Enum.map(bindings, fn %ParamBinding{} = binding ->
        if MapSet.member?(names, normalize_param_name(binding.name)) do
          %ParamBinding{binding | value: :redacted}
        else
          binding
        end
      end)

    %Render{render | params: %Params{params | bindings: bindings}}
  end

  @spec redact_write_plan(WritePlan.t() | nil, Resolution.t()) :: WritePlan.t() | nil
  def redact_write_plan(nil, %Resolution{}), do: nil

  def redact_write_plan(%WritePlan{} = write_plan, %Resolution{sensitive_params: []}),
    do: write_plan

  def redact_write_plan(%WritePlan{} = write_plan, %Resolution{}),
    do: %WritePlan{write_plan | params: :redacted}

  defp ensure_runtime_module(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        if function_exported?(module, :resolve, 1) do
          :ok
        else
          {:error,
           runtime_error(
             :runtime_inputs_missing_callback,
             "runtime input resolver does not export resolve/1",
             %{resolver: module}
           )}
        end

      {:error, _reason} ->
        {:error,
         runtime_error(
           :runtime_inputs_missing_module,
           "runtime input resolver module is unavailable",
           %{resolver: module}
         )}
    end
  end

  defp invoke(module, context, timeout_ms) do
    parent = self()
    token = make_ref()

    {proxy, monitor_ref} =
      spawn_monitor(fn -> resolver_proxy(parent, token, module, context, timeout_ms) end)

    receive do
      {^token, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^proxy, reason} ->
        {:error,
         runtime_error(
           :runtime_inputs_raised,
           "runtime input resolver process terminated unexpectedly",
           %{resolver: module, kind: exit_kind(reason)}
         )}
    after
      timeout_ms + 1_000 ->
        Process.exit(proxy, :kill)

        {:error,
         runtime_error(
           :runtime_inputs_timeout,
           "runtime input resolver exceeded its execution deadline",
           %{resolver: module, timeout_ms: timeout_ms}
         )}
    end
  end

  defp resolver_proxy(parent, token, module, context, timeout_ms) do
    Process.flag(:trap_exit, true)
    started_at = System.monotonic_time(:millisecond)
    parent_ref = Process.monitor(parent)
    proxy = self()

    worker =
      spawn_link(fn ->
        send(proxy, {:resolver_result, self(), capture_resolver(module, context)})
      end)

    receive do
      {:resolver_result, ^worker, result} ->
        send(parent, {token, result})

      {:EXIT, ^worker, reason} ->
        send(
          parent,
          {token,
           {:error,
            runtime_error(
              :runtime_inputs_raised,
              "runtime input resolver exited unexpectedly",
              %{resolver: module, kind: exit_kind(reason)}
            )}}
        )

      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        stop_worker(worker)

        :telemetry.execute(
          [:favn, :sql_asset, :runtime_inputs],
          %{duration_ms: max(System.monotonic_time(:millisecond) - started_at, 0)},
          %{
            resolver: module,
            asset_ref: context.asset.ref,
            outcome: :runtime_inputs_cancelled
          }
        )
    after
      timeout_ms ->
        stop_worker(worker)

        send(
          parent,
          {token,
           {:error,
            runtime_error(
              :runtime_inputs_timeout,
              "runtime input resolver exceeded its execution deadline",
              %{resolver: module, timeout_ms: timeout_ms}
            )}}
        )
    end
  end

  defp stop_worker(worker) do
    Process.exit(worker, :kill)

    receive do
      {:EXIT, ^worker, _reason} -> :ok
    after
      100 -> :ok
    end
  end

  defp capture_resolver(module, context) do
    {:ok, apply(module, :resolve, [context])}
  rescue
    error ->
      {:error,
       runtime_error(
         :runtime_inputs_raised,
         "runtime input resolver raised",
         %{resolver: module, kind: :error, exception: error.__struct__}
       )}
  catch
    kind, _reason when kind in [:throw, :exit] ->
      {:error,
       runtime_error(
         :runtime_inputs_raised,
         "runtime input resolver terminated abnormally",
         %{resolver: module, kind: kind}
       )}
  end

  defp validate_resolver_result({:ok, %Result{} = result}, module) do
    with :ok <- validate_result_params(result.params, module),
         :ok <- validate_identity(result.identity, module),
         :ok <- validate_metadata(result.metadata, module),
         :ok <- validate_sensitive_params(result.sensitive_params, result.params, module) do
      {:ok, result}
    end
  end

  defp validate_resolver_result({:error, %ResolverError{} = error}, module) do
    with :ok <- validate_resolver_error(error, module) do
      {:error,
       runtime_error(
         :runtime_inputs_failed,
         error.message,
         %{
           resolver: module,
           reason: error.reason,
           resolver_metadata: error.metadata,
           asset_retryable?: error.retryable?,
           retry_after_ms: error.retry_after_ms
         }
       )}
    end
  end

  defp validate_resolver_result(_other, module) do
    {:error,
     runtime_error(
       :runtime_inputs_invalid_result,
       "runtime input resolver returned an invalid result; expected {:ok, %Favn.SQLAsset.RuntimeInputs.Result{}} or {:error, %Favn.SQLAsset.RuntimeInputs.Error{}}",
       %{resolver: module}
     )}
  end

  defp validate_result_params(params, module) when is_map(params) and not is_struct(params) do
    cond do
      map_size(params) > @max_params ->
        {:error,
         runtime_error(
           :runtime_inputs_payload_too_large,
           "runtime input resolver returned more than #{@max_params} parameters",
           %{resolver: module, limit: @max_params}
         )}

      :erlang.external_size(params) > @max_param_payload_bytes ->
        {:error,
         runtime_error(
           :runtime_inputs_payload_too_large,
           "runtime input parameter payload exceeds #{@max_param_payload_bytes} bytes",
           %{resolver: module, limit_bytes: @max_param_payload_bytes}
         )}

      true ->
        validate_param_entries(params, module)
    end
  end

  defp validate_result_params(_params, module) do
    {:error,
     runtime_error(
       :runtime_inputs_invalid_result,
       "runtime input result params must be a map",
       %{resolver: module}
     )}
  end

  defp validate_param_entries(params, module) do
    normalized_names = Enum.map(Map.keys(params), &normalize_param_name/1)

    cond do
      Enum.any?(normalized_names, &match?(:error, &1)) ->
        invalid_param_error(
          module,
          "runtime input parameter names must be non-empty atoms or strings"
        )

      length(Enum.uniq(normalized_names)) != length(normalized_names) ->
        invalid_param_error(
          module,
          "runtime input parameter names must be unique after normalization"
        )

      reserved_name = Enum.find(normalized_names, &reserved_name?/1) ->
        {:error,
         runtime_error(
           :runtime_inputs_param_collision,
           "runtime input parameter #{inspect(reserved_name)} is reserved",
           %{resolver: module, parameter: reserved_name}
         )}

      invalid = Enum.find(params, fn {_name, value} -> not supported_param_value?(value) end) ->
        {name, _value} = invalid

        invalid_param_error(
          module,
          "runtime input parameter #{inspect(name)} has an unsupported value type"
        )

      true ->
        :ok
    end
  end

  defp validate_identity(identity, module)
       when is_binary(identity) and byte_size(identity) > 0 and
              byte_size(identity) <= @max_identity_bytes do
    if String.trim(identity) == "",
      do: {:error, invalid_identity_error(module, identity)},
      else: :ok
  end

  defp validate_identity(identity, module) do
    {:error, invalid_identity_error(module, identity)}
  end

  defp invalid_identity_error(module, _identity) do
    runtime_error(
      :runtime_inputs_invalid_result,
      "runtime input identity must be a non-empty string of at most #{@max_identity_bytes} bytes",
      if(module, do: %{resolver: module, limit_bytes: @max_identity_bytes}, else: %{})
    )
  end

  defp validate_metadata(metadata, module) when is_map(metadata) and not is_struct(metadata) do
    case json_safe_metadata(metadata, 0) do
      {:ok, entries} ->
        with :ok <- validate_metadata_entries(entries, module),
             {:ok, encoded} <- encode_metadata(metadata, module),
             :ok <- validate_metadata_size(encoded, module) do
          :ok
        end

      {:error, :invalid_metadata} ->
        {:error,
         runtime_error(
           :runtime_inputs_invalid_result,
           "runtime input metadata contains a non-JSON-safe value",
           %{resolver: module}
         )}
    end
  end

  defp validate_metadata(_metadata, module) do
    {:error,
     runtime_error(
       :runtime_inputs_invalid_result,
       "runtime input metadata must be a JSON-safe map",
       %{resolver: module}
     )}
  end

  defp json_safe_metadata(value, entries) when is_binary(value) or is_number(value),
    do: {:ok, entries}

  defp json_safe_metadata(value, entries) when value in [true, false, nil], do: {:ok, entries}

  defp json_safe_metadata(value, entries) when is_list(value) do
    if proper_list?(value) do
      Enum.reduce_while(value, {:ok, entries}, fn item, {:ok, count} ->
        case json_safe_metadata(item, count) do
          {:ok, next_count} -> {:cont, {:ok, next_count}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      {:error, :invalid_metadata}
    end
  end

  defp json_safe_metadata(value, entries) when is_map(value) and not is_struct(value) do
    Enum.reduce_while(value, {:ok, entries + map_size(value)}, fn {key, item}, {:ok, count} ->
      if (is_atom(key) and not is_nil(key)) or is_binary(key) do
        case json_safe_metadata(item, count) do
          {:ok, next_count} -> {:cont, {:ok, next_count}}
          {:error, _reason} = error -> {:halt, error}
        end
      else
        {:halt, {:error, :invalid_metadata}}
      end
    end)
  end

  defp json_safe_metadata(_value, _entries), do: {:error, :invalid_metadata}

  defp validate_metadata_entries(entries, module) when entries > @max_metadata_entries do
    {:error,
     runtime_error(
       :runtime_inputs_payload_too_large,
       "runtime input metadata exceeds #{@max_metadata_entries} entries",
       %{resolver: module, limit: @max_metadata_entries}
     )}
  end

  defp validate_metadata_entries(_entries, _module), do: :ok

  defp encode_metadata(metadata, module) do
    {:ok, Jason.encode!(metadata)}
  rescue
    _error ->
      {:error,
       runtime_error(
         :runtime_inputs_invalid_result,
         "runtime input metadata could not be JSON encoded",
         %{resolver: module}
       )}
  end

  defp validate_metadata_size(encoded, module) when byte_size(encoded) > @max_metadata_bytes do
    {:error,
     runtime_error(
       :runtime_inputs_payload_too_large,
       "runtime input metadata exceeds #{@max_metadata_bytes} encoded bytes",
       %{resolver: module, limit_bytes: @max_metadata_bytes}
     )}
  end

  defp validate_metadata_size(_encoded, _module), do: :ok

  defp validate_sensitive_params(names, params, module) when is_list(names) do
    normalized_names = Enum.map(names, &normalize_param_name/1)
    param_names = params |> Map.keys() |> Enum.map(&normalize_param_name/1) |> MapSet.new()

    cond do
      not proper_list?(names) or Enum.any?(normalized_names, &match?(:error, &1)) ->
        invalid_param_error(
          module,
          "sensitive_params must contain only non-empty atom or string names"
        )

      length(normalized_names) > @max_params ->
        invalid_param_error(module, "sensitive_params may contain at most #{@max_params} names")

      length(Enum.uniq(normalized_names)) != length(normalized_names) ->
        invalid_param_error(module, "sensitive_params names must be unique after normalization")

      missing = Enum.find(normalized_names, &(!MapSet.member?(param_names, &1))) ->
        invalid_param_error(
          module,
          "sensitive parameter #{inspect(missing)} is not present in params"
        )

      true ->
        :ok
    end
  end

  defp validate_sensitive_params(_names, _params, module) do
    invalid_param_error(module, "sensitive_params must be a list of parameter names")
  end

  defp validate_resolver_error(%ResolverError{} = error, module) do
    cond do
      not ((is_atom(error.reason) and not is_nil(error.reason)) or
               (is_binary(error.reason) and String.trim(error.reason) != "")) ->
        {:error,
         runtime_error(
           :runtime_inputs_invalid_result,
           "runtime input error reason must be a non-empty atom or string",
           %{resolver: module}
         )}

      not is_binary(error.message) or String.trim(error.message) == "" or
          byte_size(error.message) > @max_error_message_bytes ->
        {:error,
         runtime_error(
           :runtime_inputs_invalid_result,
           "runtime input error message must be a non-empty string of at most #{@max_error_message_bytes} bytes",
           %{resolver: module}
         )}

      not is_boolean(error.retryable?) ->
        {:error,
         runtime_error(
           :runtime_inputs_invalid_result,
           "runtime input error retryable? must be a boolean",
           %{resolver: module}
         )}

      not valid_retry_after?(error.retry_after_ms) ->
        {:error,
         runtime_error(
           :runtime_inputs_invalid_result,
           "runtime input error retry_after_ms must be nil or an integer from 0 through 86400000",
           %{resolver: module}
         )}

      true ->
        validate_metadata(error.metadata, module)
    end
  end

  defp valid_retry_after?(nil), do: true
  defp valid_retry_after?(value), do: is_integer(value) and value >= 0 and value <= 86_400_000

  defp merge_params(submitted, resolved, module) do
    submitted_names = submitted |> Map.keys() |> Enum.map(&normalize_param_name/1) |> MapSet.new()

    case Enum.find(Map.keys(resolved), &MapSet.member?(submitted_names, normalize_param_name(&1))) do
      nil ->
        {:ok, Map.merge(submitted, resolved)}

      name ->
        {:error,
         runtime_error(
           :runtime_inputs_param_collision,
           "runtime input parameter #{inspect(normalize_param_name(name))} collides with a submitted parameter",
           %{resolver: module, parameter: normalize_param_name(name)}
         )}
    end
  end

  defp supported_param_value?(value)
       when is_nil(value) or is_boolean(value) or is_number(value) or is_binary(value),
       do: true

  defp supported_param_value?(%Date{}), do: true
  defp supported_param_value?(%Time{}), do: true
  defp supported_param_value?(%NaiveDateTime{}), do: true
  defp supported_param_value?(%DateTime{}), do: true
  defp supported_param_value?(%Decimal{}), do: true
  defp supported_param_value?(_value), do: false

  defp normalize_param_name(name) when is_atom(name) and not is_nil(name),
    do: Atom.to_string(name)

  defp normalize_param_name(name) when is_binary(name) and byte_size(name) > 0, do: name
  defp normalize_param_name(_name), do: :error

  defp reserved_name?(name) do
    name in Enum.map(Template.reserved_runtime_inputs(), &Atom.to_string/1)
  end

  defp sensitive_values(params, sensitive_params) do
    names = MapSet.new(sensitive_params, &normalize_param_name/1)

    params
    |> Enum.filter(fn {name, _value} -> MapSet.member?(names, normalize_param_name(name)) end)
    |> Enum.map(&elem(&1, 1))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp resolution_timeout_ms(opts) do
    [
      @default_timeout_ms,
      positive_timeout(Keyword.get(opts, :timeout_ms)),
      deadline_timeout(Keyword.get(opts, :deadline)),
      cancel_token_timeout(Keyword.get(opts, :cancel_token))
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.min()
    |> max(1)
  end

  defp positive_timeout(timeout) when is_integer(timeout) and timeout > 0, do: timeout
  defp positive_timeout(_timeout), do: nil

  defp cancel_token_timeout(%CancelToken{deadline_at: deadline}), do: deadline_timeout(deadline)
  defp cancel_token_timeout(_token), do: nil

  defp deadline_timeout(%DateTime{} = deadline) do
    max(DateTime.diff(deadline, DateTime.utc_now(), :millisecond), 1)
  end

  defp deadline_timeout(_deadline), do: nil

  defp proper_list?([]), do: true
  defp proper_list?([_head | tail]), do: proper_list?(tail)
  defp proper_list?(_other), do: false

  defp invalid_param_error(module, message) do
    {:error,
     runtime_error(
       :runtime_inputs_invalid_result,
       message,
       %{resolver: module}
     )}
  end

  defp runtime_error(type, message, details) do
    %Error{
      type: type,
      phase: :runtime_inputs,
      message: message,
      details: Map.put_new(details, :asset_retryable?, false)
    }
  end

  defp put_duration({:ok, %Resolution{} = resolution}, duration_ms),
    do: {:ok, %Resolution{resolution | duration_ms: duration_ms}}

  defp put_duration(result, _duration_ms), do: result

  defp emit_telemetry(module, asset_ref, result, duration_ms) do
    outcome =
      case result do
        {:ok, %Resolution{}} -> :ok
        {:error, %Error{type: type}} -> type
      end

    :telemetry.execute(
      [:favn, :sql_asset, :runtime_inputs],
      %{duration_ms: duration_ms},
      %{resolver: module, asset_ref: asset_ref, outcome: outcome}
    )
  end

  defp exit_kind(:normal), do: :normal
  defp exit_kind(:killed), do: :killed
  defp exit_kind(_reason), do: :exit

  defp redact_value(%Error{} = error, sensitive_values) do
    %Error{
      error
      | message: redact_value(error.message, sensitive_values),
        details: redact_value(error.details, sensitive_values),
        cause: redact_value(error.cause, sensitive_values)
    }
  end

  defp redact_value(%SQLError{} = error, sensitive_values) do
    %SQLError{
      error
      | message: redact_value(error.message, sensitive_values),
        details: redact_value(error.details, sensitive_values),
        cause: redact_value(error.cause, sensitive_values)
    }
  end

  defp redact_value(value, sensitive_values) do
    cond do
      Enum.any?(sensitive_values, &(&1 === value)) ->
        :redacted

      is_binary(value) ->
        redact_binary(value, sensitive_values)

      is_struct(value) ->
        module = value.__struct__

        value
        |> Map.from_struct()
        |> Map.new(fn {key, child} ->
          {redact_value(key, sensitive_values), redact_value(child, sensitive_values)}
        end)
        |> then(&struct(module, &1))

      is_map(value) ->
        Map.new(value, fn {key, child} ->
          {redact_value(key, sensitive_values), redact_value(child, sensitive_values)}
        end)

      is_list(value) ->
        Enum.map(value, &redact_value(&1, sensitive_values))

      is_tuple(value) ->
        value
        |> Tuple.to_list()
        |> Enum.map(&redact_value(&1, sensitive_values))
        |> List.to_tuple()

      true ->
        value
    end
  end

  defp redact_binary(value, sensitive_values) do
    sensitive_values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.reduce(value, &String.replace(&2, &1, "[REDACTED]"))
  end
end

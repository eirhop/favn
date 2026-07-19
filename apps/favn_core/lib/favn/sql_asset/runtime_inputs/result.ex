defmodule Favn.SQLAsset.RuntimeInputs.Result do
  @moduledoc """
  Successful runtime SQL input resolution.

  `params` contains ordinary SQL bind values and `identity` names the immutable
  external input selection. `metadata` is bounded JSON-safe lineage data.
  `sensitive_params` lists parameter names whose values must be redacted from
  inspection, errors, logs, events, telemetry, and result metadata.
  Temporal parameter values must use `Calendar.ISO`; custom calendar structs
  are rejected because persistence must preserve bind values exactly.

  Read `Favn.AI`, `Favn.SQLAsset`, and `Favn.SQLAsset.RuntimeInputs` first.
  Authors return this struct from `Favn.SQLAsset.RuntimeInputs.resolve/1`; they
  do not place it in the manifest.
  """

  @enforce_keys [:params, :identity]
  defstruct params: %{}, identity: nil, metadata: %{}, sensitive_params: []

  @type param_name :: atom() | String.t()
  @type iso_date :: %Date{calendar: Calendar.ISO}
  @type iso_time :: %Time{calendar: Calendar.ISO}
  @type iso_naive_datetime :: %NaiveDateTime{calendar: Calendar.ISO}
  @type iso_datetime :: %DateTime{calendar: Calendar.ISO}
  @type param_value ::
          nil
          | boolean()
          | number()
          | String.t()
          | iso_date()
          | iso_time()
          | iso_naive_datetime()
          | iso_datetime()
          | Decimal.t()
  @type json_value ::
          nil
          | boolean()
          | number()
          | String.t()
          | [json_value()]
          | %{optional(atom() | String.t()) => json_value()}

  @type t :: %__MODULE__{
          params: %{optional(param_name()) => param_value()},
          identity: String.t(),
          metadata: %{optional(atom() | String.t()) => json_value()},
          sensitive_params: [param_name()]
        }

  @doc false
  @spec inspect_fields(t()) :: keyword()
  def inspect_fields(%__MODULE__{} = result) do
    sensitive_names = MapSet.new(result.sensitive_params, &normalize_name/1)

    sensitive_values =
      result.params
      |> Enum.filter(fn {name, _value} ->
        MapSet.member?(sensitive_names, normalize_name(name))
      end)
      |> Enum.map(&elem(&1, 1))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    params =
      Map.new(result.params, fn {name, value} ->
        value =
          if MapSet.member?(sensitive_names, normalize_name(name)), do: :redacted, else: value

        {name, value}
      end)

    [
      params: params,
      identity: redact_value(result.identity, sensitive_values),
      metadata: redact_value(result.metadata, sensitive_values),
      sensitive_params: result.sensitive_params
    ]
  end

  defp redact_value(value, sensitive_values) do
    cond do
      Enum.any?(sensitive_values, &(&1 === value)) ->
        :redacted

      is_binary(value) ->
        Enum.reduce(sensitive_values, value, fn
          sensitive, redacted when is_binary(sensitive) and sensitive != "" ->
            String.replace(redacted, sensitive, "[REDACTED]")

          _sensitive, redacted ->
            redacted
        end)

      is_map(value) ->
        Map.new(value, fn {key, child} ->
          {redact_value(key, sensitive_values), redact_value(child, sensitive_values)}
        end)

      is_list(value) ->
        Enum.map(value, &redact_value(&1, sensitive_values))

      true ->
        value
    end
  end

  defp normalize_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_name(name) when is_binary(name), do: name
  defp normalize_name(name), do: inspect(name)
end

defimpl Inspect, for: Favn.SQLAsset.RuntimeInputs.Result do
  import Inspect.Algebra

  def inspect(result, opts) do
    concat([
      "#Favn.SQLAsset.RuntimeInputs.Result<",
      to_doc(Favn.SQLAsset.RuntimeInputs.Result.inspect_fields(result), opts),
      ">"
    ])
  end
end

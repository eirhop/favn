defmodule Favn.SQLAsset.RuntimeInputs.Result do
  @moduledoc """
  Successful runtime SQL input resolution.

  `params` contains ordinary SQL bind values and `identity` names the immutable
  external input selection. `metadata` is bounded JSON-safe lineage data.
  `sensitive_params` lists parameter names whose values must be redacted from
  inspection, errors, logs, events, telemetry, and result metadata.
  """

  @enforce_keys [:params, :identity]
  defstruct params: %{}, identity: nil, metadata: %{}, sensitive_params: []

  @type param_name :: atom() | String.t()

  @type t :: %__MODULE__{
          params: %{optional(param_name()) => term()},
          identity: String.t(),
          metadata: map(),
          sensitive_params: [param_name()]
        }

  @doc false
  @spec inspect_fields(t()) :: keyword()
  def inspect_fields(%__MODULE__{} = result) do
    sensitive_names = MapSet.new(result.sensitive_params, &normalize_name/1)

    params =
      Map.new(result.params, fn {name, value} ->
        value =
          if MapSet.member?(sensitive_names, normalize_name(name)), do: :redacted, else: value

        {name, value}
      end)

    [
      params: params,
      identity: result.identity,
      metadata: result.metadata,
      sensitive_params: result.sensitive_params
    ]
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

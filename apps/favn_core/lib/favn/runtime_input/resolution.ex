defmodule Favn.RuntimeInput.Resolution do
  @moduledoc """
  Validated result of the runner's read-only runtime-input resolution phase.

  This contract crosses the runner/orchestrator boundary before SQL rendering
  or session acquisition. Its parameter payload must be persisted only through
  the dedicated runtime-input pin store.
  """

  @derive {Inspect, except: [:params]}
  @enforce_keys [
    :resolver,
    :params,
    :input_identity,
    :metadata,
    :sensitive_params,
    :payload_fingerprint
  ]
  defstruct [
    :resolver,
    :params,
    :input_identity,
    :metadata,
    :sensitive_params,
    :payload_fingerprint,
    duration_ms: 0
  ]

  @type t :: %__MODULE__{
          resolver: module(),
          params: map(),
          input_identity: String.t(),
          metadata: map(),
          sensitive_params: [atom() | String.t()],
          payload_fingerprint: String.t(),
          duration_ms: non_neg_integer()
        }

  @doc "Builds a boundary result and fingerprints its normalized parameters."
  @spec new(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_list(attrs) or is_map(attrs) do
    attrs = Map.new(attrs)
    params = Map.get(attrs, :params)
    identity = Map.get(attrs, :input_identity, Map.get(attrs, :identity))

    with :ok <- validate_module(Map.get(attrs, :resolver)),
         :ok <- validate_map(params, :params),
         :ok <- validate_identity(identity),
         :ok <- validate_map(Map.get(attrs, :metadata, %{}), :metadata),
         :ok <- validate_sensitive_params(Map.get(attrs, :sensitive_params, [])),
         :ok <- validate_duration(Map.get(attrs, :duration_ms, 0)) do
      fingerprint = Map.get(attrs, :payload_fingerprint) || fingerprint(params)

      {:ok,
       %__MODULE__{
         resolver: Map.fetch!(attrs, :resolver),
         params: params,
         input_identity: identity,
         metadata: Map.get(attrs, :metadata, %{}),
         sensitive_params: Map.get(attrs, :sensitive_params, []),
         payload_fingerprint: fingerprint,
         duration_ms: Map.get(attrs, :duration_ms, 0)
       }}
    end
  end

  @doc "Returns a stable SHA-256 fingerprint without exposing parameter values."
  @spec fingerprint(map()) :: String.t()
  def fingerprint(params) when is_map(params) do
    params
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp validate_module(module) when is_atom(module) and not is_nil(module), do: :ok
  defp validate_module(_module), do: {:error, :invalid_runtime_input_resolver}

  defp validate_map(value, _field) when is_map(value), do: :ok
  defp validate_map(_value, field), do: {:error, {:invalid_runtime_input_resolution, field}}

  defp validate_identity(value) when is_binary(value) and value != "", do: :ok
  defp validate_identity(_value), do: {:error, :invalid_runtime_input_identity}

  defp validate_sensitive_params(values) when is_list(values) do
    if Enum.all?(values, &(is_atom(&1) or is_binary(&1))),
      do: :ok,
      else: {:error, :invalid_runtime_input_sensitive_params}
  end

  defp validate_sensitive_params(_values), do: {:error, :invalid_runtime_input_sensitive_params}

  defp validate_duration(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_duration(_value), do: {:error, :invalid_runtime_input_duration}
end

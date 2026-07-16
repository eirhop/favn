defmodule Favn.RuntimeInput.Pin do
  @moduledoc """
  Run-scoped, planned-node runtime-input pin.

  Pin identity is `{run_id, planned_node_key}`. Parameters are intentionally
  excluded from `Inspect`; persistence must use a dedicated pin codec rather
  than generic run metadata or event payloads.
  """

  alias Favn.RuntimeInput.Resolution

  @derive {Inspect, except: [:params]}
  @enforce_keys [
    :run_id,
    :node_key,
    :resolver,
    :params,
    :input_identity,
    :metadata,
    :sensitive_params,
    :payload_fingerprint,
    :schema_version,
    :inserted_at,
    :updated_at
  ]
  defstruct [
    :run_id,
    :node_key,
    :resolver,
    :params,
    :input_identity,
    :metadata,
    :sensitive_params,
    :payload_fingerprint,
    :source_run_id,
    :source_node_key,
    :source_payload_fingerprint,
    :inserted_at,
    :updated_at,
    schema_version: 1
  ]

  @type t :: %__MODULE__{
          run_id: String.t(),
          node_key: Favn.Plan.node_key(),
          resolver: module(),
          params: map(),
          input_identity: String.t(),
          metadata: map(),
          sensitive_params: [atom() | String.t()],
          payload_fingerprint: String.t(),
          source_run_id: String.t() | nil,
          source_node_key: Favn.Plan.node_key() | nil,
          source_payload_fingerprint: String.t() | nil,
          schema_version: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @doc "Creates a new run/node pin from a validated runner resolution."
  @spec new(String.t(), Favn.Plan.node_key(), Resolution.t()) :: t()
  def new(run_id, node_key, %Resolution{} = resolution)
      when is_binary(run_id) and run_id != "" and is_tuple(node_key) do
    now = DateTime.utc_now()

    %__MODULE__{
      run_id: run_id,
      node_key: node_key,
      resolver: resolution.resolver,
      params: resolution.params,
      input_identity: resolution.input_identity,
      metadata: resolution.metadata,
      sensitive_params: resolution.sensitive_params,
      payload_fingerprint: resolution.payload_fingerprint,
      schema_version: 1,
      inserted_at: now,
      updated_at: now
    }
  end

  @doc "Creates a lineage-preserving inherited pin for a new run."
  @spec inherit(String.t(), Favn.Plan.node_key(), t()) :: t()
  def inherit(run_id, node_key, %__MODULE__{} = source)
      when is_binary(run_id) and run_id != "" and is_tuple(node_key) do
    now = DateTime.utc_now()

    %__MODULE__{
      run_id: run_id,
      node_key: node_key,
      resolver: source.resolver,
      params: source.params,
      input_identity: source.input_identity,
      metadata: source.metadata,
      sensitive_params: source.sensitive_params,
      payload_fingerprint: source.payload_fingerprint,
      source_run_id: source.run_id,
      source_node_key: source.node_key,
      source_payload_fingerprint: source.payload_fingerprint,
      schema_version: 1,
      inserted_at: now,
      updated_at: now
    }
  end

  @doc "Returns safe lineage fields suitable for public run details."
  @spec lineage(t()) :: map()
  def lineage(%__MODULE__{} = pin) do
    %{
      node_key: pin.node_key,
      resolver: pin.resolver,
      input_identity: pin.input_identity,
      payload_fingerprint: pin.payload_fingerprint,
      source_run_id: pin.source_run_id,
      source_node_key: pin.source_node_key,
      source_payload_fingerprint: pin.source_payload_fingerprint
    }
  end

  @doc "Returns true when two racing pin candidates select the same payload."
  @spec equivalent?(t(), t()) :: boolean()
  def equivalent?(%__MODULE__{} = left, %__MODULE__{} = right) do
    left.resolver == right.resolver and left.input_identity == right.input_identity and
      left.payload_fingerprint == right.payload_fingerprint
  end
end

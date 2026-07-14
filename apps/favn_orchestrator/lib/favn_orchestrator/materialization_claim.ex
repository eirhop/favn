defmodule FavnOrchestrator.MaterializationClaim do
  @moduledoc """
  Storage-backed claim for one logical asset materialization identity.

  A claim is keyed by an execution-supplied deterministic `claim_key`. Storage
  adapters use that key to prevent duplicate materialization for the same asset,
  freshness, manifest producer identity, and input fingerprint while preserving
  enough terminal state for crash recovery to skip already-succeeded reusable work
  or reclaim expired/failed work.
  """

  @enforce_keys [
    :claim_key,
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :input_fingerprint,
    :status,
    :claimed_at,
    :expires_at
  ]
  defstruct [
    :claim_key,
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :input_fingerprint,
    :run_id,
    :asset_step_id,
    :node_key,
    :runner_execution_id,
    :manifest_version_id,
    :manifest_content_hash,
    :freshness_version,
    :status,
    :error,
    :claimed_at,
    :heartbeat_at,
    :expires_at,
    :finished_at,
    metadata: %{}
  ]

  @type status :: :claimed | :succeeded | :failed | :cancelled | :timed_out | :expired

  @type t :: %__MODULE__{
          claim_key: String.t(),
          asset_ref_module: module(),
          asset_ref_name: atom(),
          freshness_key: String.t(),
          input_fingerprint: String.t(),
          run_id: String.t() | nil,
          asset_step_id: String.t() | nil,
          node_key: term() | nil,
          runner_execution_id: String.t() | nil,
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          freshness_version: String.t() | nil,
          status: status(),
          error: term() | nil,
          claimed_at: DateTime.t(),
          heartbeat_at: DateTime.t() | nil,
          expires_at: DateTime.t(),
          finished_at: DateTime.t() | nil,
          metadata: map()
        }

  @statuses [:claimed, :succeeded, :failed, :cancelled, :timed_out, :expired]
  @terminal_failure_statuses [:failed, :cancelled, :timed_out, :expired]
  @optional_binary_fields [
    :run_id,
    :asset_step_id,
    :runner_execution_id,
    :manifest_version_id,
    :manifest_content_hash,
    :freshness_version
  ]

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_required(attrs),
         :ok <- validate_identity(attrs),
         :ok <- validate_optional_binaries(attrs),
         :ok <- validate_timestamps(attrs),
         :ok <- validate_metadata(Map.get(attrs, :metadata, %{})) do
      {:ok, struct(__MODULE__, Map.merge(%{metadata: %{}}, attrs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_materialization_claim}

  @spec active?(t(), DateTime.t()) :: boolean()
  def active?(
        %__MODULE__{status: :claimed, expires_at: %DateTime{} = expires_at},
        %DateTime{} = now
      ) do
    DateTime.compare(expires_at, now) == :gt
  end

  def active?(%__MODULE__{}, %DateTime{}), do: false

  @spec terminal_failure_status?(status()) :: boolean()
  def terminal_failure_status?(status), do: status in @terminal_failure_statuses

  @spec statuses() :: [status()]
  def statuses, do: @statuses

  @spec terminal_failure_statuses() :: [status()]
  def terminal_failure_statuses, do: @terminal_failure_statuses

  defp validate_required(attrs) do
    required = [
      :claim_key,
      :asset_ref_module,
      :asset_ref_name,
      :freshness_key,
      :input_fingerprint,
      :status,
      :claimed_at,
      :expires_at
    ]

    case Enum.filter(required, &(Map.get(attrs, &1) in [nil, ""])) do
      [] -> :ok
      missing -> {:error, {:missing_required_keys, missing}}
    end
  end

  defp validate_identity(attrs) do
    cond do
      not is_binary(attrs.claim_key) or attrs.claim_key == "" ->
        {:error, {:invalid_claim_key, attrs.claim_key}}

      not is_atom(attrs.asset_ref_module) ->
        {:error, {:invalid_asset_ref_module, attrs.asset_ref_module}}

      not is_atom(attrs.asset_ref_name) ->
        {:error, {:invalid_asset_ref_name, attrs.asset_ref_name}}

      not is_binary(attrs.freshness_key) or attrs.freshness_key == "" ->
        {:error, {:invalid_freshness_key, attrs.freshness_key}}

      not is_binary(attrs.input_fingerprint) or attrs.input_fingerprint == "" ->
        {:error, {:invalid_input_fingerprint, attrs.input_fingerprint}}

      true ->
        :ok
    end
  end

  defp normalize_attrs(attrs) do
    with {:ok, status} <- normalize_status(field_value(attrs, :status)),
         {:ok, claimed_at} <- normalize_datetime(field_value(attrs, :claimed_at)),
         {:ok, heartbeat_at} <- normalize_optional_datetime(field_value(attrs, :heartbeat_at)),
         {:ok, expires_at} <- normalize_datetime(field_value(attrs, :expires_at)),
         {:ok, finished_at} <- normalize_optional_datetime(field_value(attrs, :finished_at)) do
      {:ok,
       attrs
       |> atomize_known_keys()
       |> Map.put(:status, status)
       |> Map.put(:claimed_at, claimed_at)
       |> Map.put(:heartbeat_at, heartbeat_at)
       |> Map.put(:expires_at, expires_at)
       |> Map.put(:finished_at, finished_at)}
    end
  end

  defp atomize_known_keys(attrs) do
    Enum.reduce(attrs, %{}, fn {key, value}, acc ->
      Map.put(acc, atomize_key(key), value)
    end)
  end

  defp atomize_key(key) when is_atom(key), do: key

  defp atomize_key(key) when is_binary(key) do
    Enum.find(__MODULE__.__struct__() |> Map.keys(), key, &(Atom.to_string(&1) == key)) || key
  end

  defp normalize_status(value) when value in @statuses, do: {:ok, value}

  defp normalize_status(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> case do
      "claimed" -> {:ok, :claimed}
      "succeeded" -> {:ok, :succeeded}
      "failed" -> {:ok, :failed}
      "cancelled" -> {:ok, :cancelled}
      "timed_out" -> {:ok, :timed_out}
      "expired" -> {:ok, :expired}
      other -> {:error, {:invalid_status, other}}
    end
  end

  defp normalize_status(value), do: {:error, {:invalid_status, value}}

  defp normalize_optional_datetime(nil), do: {:ok, nil}
  defp normalize_optional_datetime(""), do: {:ok, nil}
  defp normalize_optional_datetime(value), do: normalize_datetime(value)

  defp normalize_datetime(%DateTime{} = value), do: {:ok, value}

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, {:invalid_datetime, value}}
    end
  end

  defp normalize_datetime(value), do: {:error, {:invalid_datetime, value}}

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(value), do: {:error, {:invalid_metadata, value}}

  defp validate_optional_binaries(attrs) do
    Enum.reduce_while(@optional_binary_fields, :ok, fn field, :ok ->
      case Map.get(attrs, field) do
        nil -> {:cont, :ok}
        value when is_binary(value) and value != "" -> {:cont, :ok}
        value -> {:halt, {:error, {:invalid_materialization_claim_field, field, value}}}
      end
    end)
  end

  defp validate_timestamps(attrs) do
    claimed_at = Map.fetch!(attrs, :claimed_at)
    expires_at = Map.fetch!(attrs, :expires_at)

    cond do
      DateTime.compare(expires_at, claimed_at) != :gt ->
        {:error, {:invalid_materialization_claim_range, :expires_at, claimed_at, expires_at}}

      not timestamp_after_claim?(Map.get(attrs, :heartbeat_at), claimed_at) ->
        {:error,
         {:invalid_materialization_claim_range, :heartbeat_at, claimed_at,
          Map.get(attrs, :heartbeat_at)}}

      not timestamp_after_claim?(Map.get(attrs, :finished_at), claimed_at) ->
        {:error,
         {:invalid_materialization_claim_range, :finished_at, claimed_at,
          Map.get(attrs, :finished_at)}}

      true ->
        :ok
    end
  end

  defp timestamp_after_claim?(nil, _claimed_at), do: true

  defp timestamp_after_claim?(%DateTime{} = timestamp, %DateTime{} = claimed_at),
    do: DateTime.compare(timestamp, claimed_at) in [:eq, :gt]

  defp field_value(map, field), do: Map.get(map, field) || Map.get(map, Atom.to_string(field))
end

defmodule FavnOrchestrator.Backfill.CoverageBaseline do
  @moduledoc """
  Normalized coverage/cutover state established by a full-load or baseline run.

  Source identity fields are intentionally limited to stable keys plus hashed or
  redacted values. Raw source IDs, tokens, and secrets must not be stored here.

  Backfill range resolution can use this state to choose relative windows after
  a safe baseline/cutover point. The baseline is derived state; the source run
  and run event stream remain authoritative.
  """

  @enforce_keys [
    :baseline_id,
    :pipeline_module,
    :source_key,
    :segment_key_hash,
    :window_kind,
    :timezone,
    :coverage_until,
    :created_by_run_id,
    :manifest_version_id,
    :status,
    :created_at,
    :updated_at
  ]
  defstruct [
    :baseline_id,
    :pipeline_module,
    :source_key,
    :segment_key_hash,
    :segment_key_redacted,
    :window_kind,
    :timezone,
    :coverage_start_at,
    :coverage_until,
    :created_by_run_id,
    :manifest_version_id,
    :status,
    errors: [],
    metadata: %{},
    created_at: nil,
    updated_at: nil
  ]

  @type status :: :pending | :ok | :error | :cancelled | atom()

  @type t :: %__MODULE__{
          baseline_id: String.t(),
          pipeline_module: module(),
          source_key: String.t(),
          segment_key_hash: String.t(),
          segment_key_redacted: String.t() | nil,
          window_kind: atom(),
          timezone: String.t(),
          coverage_start_at: DateTime.t() | nil,
          coverage_until: DateTime.t(),
          created_by_run_id: String.t(),
          manifest_version_id: String.t(),
          status: status(),
          errors: [term()],
          metadata: map(),
          created_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys
  @raw_source_keys [:segment_id, :source_id, :source_secret, :token, :secret]

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with :ok <- reject_raw_source_identity(attrs),
         :ok <- require_keys(attrs, @required_keys) do
      {:ok,
       struct(
         __MODULE__,
         Map.merge(%{errors: [], metadata: %{}}, attrs)
       )}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  defp reject_raw_source_identity(attrs) do
    case Enum.find(@raw_source_keys, &Map.has_key?(attrs, &1)) do
      nil -> :ok
      key -> {:error, {:raw_source_identity_not_allowed, key}}
    end
  end

  defp require_keys(attrs, keys) do
    missing = Enum.filter(keys, &missing?(attrs, &1))

    case missing do
      [] -> :ok
      keys -> {:error, {:missing_required_keys, keys}}
    end
  end

  defp missing?(attrs, key), do: Map.get(attrs, key) in [nil, ""]
end

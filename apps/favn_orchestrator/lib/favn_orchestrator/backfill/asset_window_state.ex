defmodule FavnOrchestrator.Backfill.AssetWindowState do
  @moduledoc """
  Latest normalized state for a concrete asset/window.

  Backfill child runs project terminal asset results into this row. It is the
  control-plane read model for questions such as "what last happened for this
  asset in this window?" and preserves the latest successful run separately from
  the latest attempt.
  """

  @enforce_keys [
    :asset_ref_module,
    :asset_ref_name,
    :pipeline_module,
    :manifest_version_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :latest_run_id,
    :updated_at
  ]
  defstruct [
    :asset_ref_module,
    :asset_ref_name,
    :pipeline_module,
    :manifest_version_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :latest_run_id,
    :latest_parent_run_id,
    :latest_success_run_id,
    :latest_error,
    :rows_written,
    :updated_at,
    errors: [],
    metadata: %{}
  ]

  @type status :: :pending | :running | :ok | :error | :cancelled | atom()

  @type t :: %__MODULE__{
          asset_ref_module: module(),
          asset_ref_name: atom(),
          pipeline_module: module(),
          manifest_version_id: String.t(),
          window_kind: atom(),
          window_start_at: DateTime.t(),
          window_end_at: DateTime.t(),
          timezone: String.t(),
          window_key: String.t(),
          status: status(),
          latest_run_id: String.t(),
          latest_parent_run_id: String.t() | nil,
          latest_success_run_id: String.t() | nil,
          latest_error: term(),
          errors: [term()],
          rows_written: non_neg_integer() | nil,
          metadata: map(),
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with :ok <- require_keys(attrs, @required_keys) do
      {:ok, struct(__MODULE__, Map.merge(%{errors: [], metadata: %{}}, attrs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  defp require_keys(attrs, keys) do
    missing = Enum.filter(keys, &missing?(attrs, &1))

    case missing do
      [] -> :ok
      keys -> {:error, {:missing_required_keys, keys}}
    end
  end

  defp missing?(attrs, key), do: Map.get(attrs, key) in [nil, ""]
end

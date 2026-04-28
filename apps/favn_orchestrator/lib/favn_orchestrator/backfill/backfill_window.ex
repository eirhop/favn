defmodule FavnOrchestrator.Backfill.BackfillWindow do
  @moduledoc """
  Normalized ledger row for one requested window in a parent backfill run.

  The row is keyed by parent backfill run id, pipeline module, and encoded
  window key. It tracks the latest child attempt, terminal status, errors, and
  timestamps so operator surfaces can read backfill progress without scanning raw
  run event streams.
  """

  @enforce_keys [
    :backfill_run_id,
    :pipeline_module,
    :manifest_version_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :updated_at
  ]
  defstruct [
    :backfill_run_id,
    :child_run_id,
    :pipeline_module,
    :manifest_version_id,
    :coverage_baseline_id,
    :window_kind,
    :window_start_at,
    :window_end_at,
    :timezone,
    :window_key,
    :status,
    :latest_attempt_run_id,
    :last_success_run_id,
    :last_error,
    :started_at,
    :finished_at,
    :created_at,
    :updated_at,
    attempt_count: 0,
    errors: [],
    metadata: %{}
  ]

  @type status :: :pending | :running | :ok | :error | :cancelled | atom()

  @type t :: %__MODULE__{
          backfill_run_id: String.t(),
          child_run_id: String.t() | nil,
          pipeline_module: module(),
          manifest_version_id: String.t(),
          coverage_baseline_id: String.t() | nil,
          window_kind: atom(),
          window_start_at: DateTime.t(),
          window_end_at: DateTime.t(),
          timezone: String.t(),
          window_key: String.t(),
          status: status(),
          attempt_count: non_neg_integer(),
          latest_attempt_run_id: String.t() | nil,
          last_success_run_id: String.t() | nil,
          last_error: term(),
          errors: [term()],
          metadata: map(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          created_at: DateTime.t() | nil,
          updated_at: DateTime.t()
        }

  @required_keys @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with :ok <- require_keys(attrs, @required_keys) do
      {:ok, struct(__MODULE__, Map.merge(%{attempt_count: 0, errors: [], metadata: %{}}, attrs))}
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

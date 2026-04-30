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

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out

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

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- require_keys(attrs, @required_keys) do
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

  defp normalize_attrs(attrs) do
    with {:ok, window_kind} <- normalize_window_kind(Map.get(attrs, :window_kind)),
         {:ok, status} <- normalize_status(Map.get(attrs, :status)) do
      {:ok, attrs |> Map.put(:window_kind, window_kind) |> Map.put(:status, status)}
    end
  end

  defp normalize_window_kind(value) when value in [:hour, :day, :month, :year], do: {:ok, value}
  defp normalize_window_kind(:hourly), do: {:ok, :hour}
  defp normalize_window_kind(:daily), do: {:ok, :day}
  defp normalize_window_kind(:monthly), do: {:ok, :month}
  defp normalize_window_kind(:yearly), do: {:ok, :year}
  defp normalize_window_kind("hour"), do: {:ok, :hour}
  defp normalize_window_kind("hourly"), do: {:ok, :hour}
  defp normalize_window_kind("day"), do: {:ok, :day}
  defp normalize_window_kind("daily"), do: {:ok, :day}
  defp normalize_window_kind("month"), do: {:ok, :month}
  defp normalize_window_kind("monthly"), do: {:ok, :month}
  defp normalize_window_kind("year"), do: {:ok, :year}
  defp normalize_window_kind("yearly"), do: {:ok, :year}
  defp normalize_window_kind(value), do: {:error, {:invalid_window_kind, value}}

  defp normalize_status(value)
       when value in [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out],
       do: {:ok, value}

  defp normalize_status(value) when is_binary(value) do
    case value do
      "pending" -> {:ok, :pending}
      "running" -> {:ok, :running}
      "ok" -> {:ok, :ok}
      "partial" -> {:ok, :partial}
      "error" -> {:ok, :error}
      "cancelled" -> {:ok, :cancelled}
      "timed_out" -> {:ok, :timed_out}
      _other -> {:error, {:invalid_status, value}}
    end
  end

  defp normalize_status(value), do: {:error, {:invalid_status, value}}
end

defmodule FavnOrchestrator.Backfill.BackfillWindow do
  @moduledoc """
  Normalized ledger row for one requested window in a parent backfill run.

  The row is keyed by parent backfill run id, pipeline module, and encoded
  window key. It tracks the latest child attempt, terminal status, errors, and
  timestamps so operator surfaces can read backfill progress without scanning raw
  run event streams.
  """

  alias FavnOrchestrator.Backfill.ReadModelValues

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

  @type status :: ReadModelValues.status()

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
         :ok <- require_keys(attrs, @required_keys),
         :ok <- validate_fields(attrs) do
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
    with {:ok, window_kind} <- ReadModelValues.normalize_window_kind(Map.get(attrs, :window_kind)),
         {:ok, status} <- ReadModelValues.normalize_status(Map.get(attrs, :status)),
         {:ok, window_start_at} <- normalize_datetime(Map.get(attrs, :window_start_at)),
         {:ok, window_end_at} <- normalize_datetime(Map.get(attrs, :window_end_at)),
         {:ok, started_at} <- normalize_optional_datetime(Map.get(attrs, :started_at)),
         {:ok, finished_at} <- normalize_optional_datetime(Map.get(attrs, :finished_at)),
         {:ok, created_at} <- normalize_optional_datetime(Map.get(attrs, :created_at)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)) do
      {:ok,
       attrs
       |> Map.put(:window_kind, window_kind)
       |> Map.put(:status, status)
       |> Map.put(:window_start_at, window_start_at)
       |> Map.put(:window_end_at, window_end_at)
       |> Map.put(:started_at, started_at)
       |> Map.put(:finished_at, finished_at)
       |> Map.put(:created_at, created_at)
       |> Map.put(:updated_at, updated_at)}
    end
  end

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

  defp validate_fields(attrs) do
    cond do
      not is_atom(attrs.pipeline_module) ->
        {:error, {:invalid_pipeline_module, attrs.pipeline_module}}

      not required_binary_fields?(attrs) ->
        {:error, :invalid_backfill_window_identity}

      not optional_binary_fields?(attrs) ->
        {:error, :invalid_backfill_window_run_identity}

      not valid_window_range?(attrs.window_start_at, attrs.window_end_at) ->
        {:error, {:invalid_window_range, attrs.window_start_at, attrs.window_end_at}}

      not valid_attempt_count?(Map.get(attrs, :attempt_count, 0)) ->
        {:error, {:invalid_attempt_count, Map.get(attrs, :attempt_count)}}

      not is_list(Map.get(attrs, :errors, [])) ->
        {:error, {:invalid_errors, Map.get(attrs, :errors)}}

      not is_map(Map.get(attrs, :metadata, %{})) ->
        {:error, {:invalid_metadata, Map.get(attrs, :metadata)}}

      true ->
        :ok
    end
  end

  defp required_binary_fields?(attrs) do
    Enum.all?([:backfill_run_id, :manifest_version_id, :timezone, :window_key], fn field ->
      value = Map.get(attrs, field)
      is_binary(value) and value != ""
    end)
  end

  defp optional_binary_fields?(attrs) do
    Enum.all?(
      [:child_run_id, :coverage_baseline_id, :latest_attempt_run_id, :last_success_run_id],
      &(is_nil(Map.get(attrs, &1)) or
          (is_binary(Map.get(attrs, &1)) and Map.get(attrs, &1) != ""))
    )
  end

  defp valid_attempt_count?(value), do: is_integer(value) and value >= 0

  defp valid_window_range?(%DateTime{} = start_at, %DateTime{} = end_at),
    do: DateTime.compare(start_at, end_at) == :lt
end

defmodule FavnOrchestrator.Backfill.Progress do
  @moduledoc """
  Persisted aggregate progress for one parent backfill run.

  Backfill windows remain the detailed ledger. This struct stores the derived
  count summary needed to update parent run status without repeatedly loading
  every window into the orchestrator process.
  """

  alias FavnOrchestrator.Backfill.BackfillWindow

  @statuses [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out]

  @enforce_keys [
    :backfill_run_id,
    :total_count,
    :pending_count,
    :running_count,
    :ok_count,
    :partial_count,
    :error_count,
    :cancelled_count,
    :timed_out_count,
    :status,
    :updated_at
  ]

  defstruct [
    :backfill_run_id,
    :total_count,
    :pending_count,
    :running_count,
    :ok_count,
    :partial_count,
    :error_count,
    :cancelled_count,
    :timed_out_count,
    :status,
    :updated_at,
    metadata: %{}
  ]

  @type status :: BackfillWindow.status()

  @type counts :: %{
          optional(status()) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          backfill_run_id: String.t(),
          total_count: non_neg_integer(),
          pending_count: non_neg_integer(),
          running_count: non_neg_integer(),
          ok_count: non_neg_integer(),
          partial_count: non_neg_integer(),
          error_count: non_neg_integer(),
          cancelled_count: non_neg_integer(),
          timed_out_count: non_neg_integer(),
          status: status(),
          updated_at: DateTime.t(),
          metadata: map()
        }

  @doc "Builds a progress struct from explicit count fields."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- require_keys(attrs, @enforce_keys),
         :ok <- validate_counts(attrs),
         :ok <- validate_total_count(attrs),
         :ok <- validate_status(Map.fetch!(attrs, :status)),
         :ok <- validate_metadata(Map.get(attrs, :metadata, %{})) do
      {:ok, struct(__MODULE__, Map.merge(%{metadata: %{}}, attrs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_attrs}

  @doc "Builds progress from a map of window-status counts."
  @spec from_counts(String.t(), counts(), DateTime.t(), map()) :: {:ok, t()} | {:error, term()}
  def from_counts(backfill_run_id, counts, %DateTime{} = updated_at, metadata \\ %{})
      when is_binary(backfill_run_id) and is_map(counts) do
    normalized_counts = normalize_counts(counts)
    total_count = Enum.reduce(normalized_counts, 0, fn {_status, count}, acc -> acc + count end)

    new(%{
      backfill_run_id: backfill_run_id,
      total_count: total_count,
      pending_count: Map.fetch!(normalized_counts, :pending),
      running_count: Map.fetch!(normalized_counts, :running),
      ok_count: Map.fetch!(normalized_counts, :ok),
      partial_count: Map.fetch!(normalized_counts, :partial),
      error_count: Map.fetch!(normalized_counts, :error),
      cancelled_count: Map.fetch!(normalized_counts, :cancelled),
      timed_out_count: Map.fetch!(normalized_counts, :timed_out),
      status: status_from_counts(total_count, normalized_counts),
      updated_at: updated_at,
      metadata: metadata
    })
  end

  @doc "Builds progress by counting a backfill's window rows."
  @spec from_windows(String.t(), [BackfillWindow.t()], DateTime.t()) ::
          {:ok, t()} | {:error, term()}
  def from_windows(backfill_run_id, windows, %DateTime{} = updated_at)
      when is_binary(backfill_run_id) and is_list(windows) do
    counts =
      Enum.reduce(windows, %{}, fn %BackfillWindow{status: status}, acc ->
        Map.update(acc, status, 1, &(&1 + 1))
      end)

    from_counts(backfill_run_id, counts, updated_at)
  end

  @doc "Returns the count map used in run transition metadata."
  @spec window_counts(t()) :: %{status() => non_neg_integer()}
  def window_counts(%__MODULE__{} = progress) do
    progress
    |> counts()
    |> Enum.reject(fn {_status, count} -> count == 0 end)
    |> Map.new()
  end

  @doc "Returns all status counts, including zero values."
  @spec counts(t()) :: counts()
  def counts(%__MODULE__{} = progress) do
    %{
      pending: progress.pending_count,
      running: progress.running_count,
      ok: progress.ok_count,
      partial: progress.partial_count,
      error: progress.error_count,
      cancelled: progress.cancelled_count,
      timed_out: progress.timed_out_count
    }
  end

  @doc false
  @spec apply_status_change(t(), status() | nil, status(), DateTime.t()) ::
          {:ok, t()} | {:error, term()}
  def apply_status_change(%__MODULE__{} = progress, old_status, new_status, %DateTime{} = now)
      when not (is_nil(old_status) or old_status in @statuses) do
    {:error, {:invalid_old_status, progress.backfill_run_id, old_status, new_status, now}}
  end

  def apply_status_change(%__MODULE__{} = progress, old_status, new_status, %DateTime{} = now)
      when new_status not in @statuses do
    {:error, {:invalid_new_status, progress.backfill_run_id, old_status, new_status, now}}
  end

  def apply_status_change(%__MODULE__{} = progress, old_status, new_status, %DateTime{} = now) do
    counts = counts(progress)

    case status_change_counts(counts, progress.total_count, old_status, new_status) do
      {:ok, counts, total_count} ->
        from_counts(
          progress.backfill_run_id,
          Map.put(counts, :__total__, total_count),
          now,
          progress.metadata
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp status_change_counts(counts, total_count, nil, new_status),
    do: {:ok, Map.update!(counts, new_status, &(&1 + 1)), total_count + 1}

  defp status_change_counts(counts, total_count, old_status, old_status),
    do: {:ok, counts, total_count}

  defp status_change_counts(counts, total_count, old_status, new_status) do
    if Map.fetch!(counts, old_status) > 0 do
      counts =
        counts
        |> Map.update!(old_status, &(&1 - 1))
        |> Map.update!(new_status, &(&1 + 1))

      {:ok, counts, total_count}
    else
      {:error, {:stale_backfill_progress, old_status, new_status, counts}}
    end
  end

  defp normalize_attrs(attrs) do
    with {:ok, status} <- normalize_status(Map.get(attrs, :status)),
         {:ok, updated_at} <- normalize_datetime(Map.get(attrs, :updated_at)) do
      {:ok,
       attrs
       |> Map.put(:status, status)
       |> Map.put(:updated_at, updated_at)
       |> normalize_count_fields()}
    end
  end

  defp normalize_count_fields(attrs) do
    Enum.reduce(count_field_keys(), attrs, fn key, acc ->
      Map.put(acc, key, normalize_count(Map.get(acc, key, 0)))
    end)
  end

  defp require_keys(attrs, keys) do
    missing = Enum.filter(keys, &(Map.get(attrs, &1) in [nil, ""]))

    case missing do
      [] -> :ok
      keys -> {:error, {:missing_required_keys, keys}}
    end
  end

  defp validate_counts(attrs) do
    count_field_keys()
    |> Enum.find_value(:ok, fn key ->
      count = Map.fetch!(attrs, key)
      if is_integer(count) and count >= 0, do: false, else: {:error, {:invalid_count, key, count}}
    end)
  end

  defp validate_total_count(attrs) do
    total = Map.fetch!(attrs, :total_count)

    sum =
      attrs
      |> Map.take([
        :pending_count,
        :running_count,
        :ok_count,
        :partial_count,
        :error_count,
        :cancelled_count,
        :timed_out_count
      ])
      |> Map.values()
      |> Enum.sum()

    if total == sum, do: :ok, else: {:error, {:invalid_total_count, total, sum}}
  end

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(status), do: {:error, {:invalid_status, status}}

  defp validate_metadata(metadata) when is_map(metadata), do: :ok
  defp validate_metadata(metadata), do: {:error, {:invalid_metadata, metadata}}

  defp normalize_counts(counts) do
    base = Map.new(@statuses, &{&1, 0})

    Enum.reduce(counts, base, fn
      {:__total__, _total}, acc -> acc
      {status, count}, acc -> Map.put(acc, normalize_status_key(status), normalize_count(count))
    end)
  end

  defp status_from_counts(0, _counts), do: :running

  defp status_from_counts(total, counts) do
    cond do
      Map.fetch!(counts, :pending) > 0 or Map.fetch!(counts, :running) > 0 -> :running
      Map.fetch!(counts, :ok) == total -> :ok
      Map.fetch!(counts, :cancelled) == total -> :cancelled
      Map.fetch!(counts, :timed_out) == total -> :timed_out
      Map.fetch!(counts, :ok) > 0 or Map.fetch!(counts, :partial) > 0 -> :partial
      true -> :error
    end
  end

  defp normalize_status(value) when value in @statuses, do: {:ok, value}

  defp normalize_status(value) when is_binary(value),
    do: value |> String.to_atom() |> normalize_status()

  defp normalize_status(value), do: {:error, {:invalid_status, value}}

  defp normalize_status_key(value) when value in @statuses, do: value
  defp normalize_status_key(value) when is_binary(value), do: String.to_atom(value)

  defp normalize_count(value) when is_integer(value), do: value
  defp normalize_count(value) when is_binary(value), do: String.to_integer(value)
  defp normalize_count(_value), do: 0

  defp normalize_datetime(%DateTime{} = value), do: {:ok, value}

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, {:invalid_datetime, value}}
    end
  end

  defp normalize_datetime(value), do: {:error, {:invalid_datetime, value}}

  defp count_field_keys do
    [
      :total_count,
      :pending_count,
      :running_count,
      :ok_count,
      :partial_count,
      :error_count,
      :cancelled_count,
      :timed_out_count
    ]
  end
end

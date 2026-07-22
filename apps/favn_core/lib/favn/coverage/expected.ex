defmodule Favn.Coverage.Expected do
  @moduledoc """
  Pure bounded evaluation of canonical windows expected by effective coverage.

  The evaluator never reads storage or the system clock. Callers provide the
  evaluation instant and page expected windows before comparing them with
  generation-scoped success evidence.
  """

  alias Favn.Coverage.Effective
  alias Favn.Manifest.Serializer
  alias Favn.TimePeriod
  alias Favn.Window.Anchor
  alias Favn.Window.Key

  @max_windows 100_000

  @type evaluation :: %{
          required(:coverage) => Effective.t(),
          required(:evaluated_at) => DateTime.t(),
          required(:first_window) => Anchor.t(),
          required(:last_expected_window) => Anchor.t() | nil,
          required(:expected_count) => non_neg_integer(),
          required(:checksum) => String.t()
        }

  @type page :: %{
          required(:items) => [Anchor.t()],
          required(:has_more?) => boolean(),
          required(:next_after) => Key.t() | nil
        }

  @doc "Evaluates expected bounds and count at an explicit instant."
  @spec evaluate(Effective.t(), DateTime.t()) ::
          {:ok, evaluation()} | {:error, :coverage_window_limit_exceeded | term()}
  def evaluate(%Effective{} = coverage, %DateTime{} = evaluated_at) do
    with {:ok, coverage} <- Effective.validate(coverage),
         {:ok, last_period} <- last_expected_period(coverage, evaluated_at),
         last_period <- expected_last_period(coverage.effective_from, last_period),
         {:ok, first_window} <- anchor(coverage.effective_from),
         {:ok, last_window} <- optional_anchor(last_period),
         {:ok, count} <- count(first_window, last_window) do
      evaluation = %{
        coverage: coverage,
        evaluated_at: evaluated_at,
        first_window: first_window,
        last_expected_window: last_window,
        expected_count: count
      }

      {:ok, Map.put(evaluation, :checksum, checksum(evaluation))}
    end
  end

  def evaluate(_coverage, _evaluated_at), do: {:error, :invalid_coverage_evaluation}

  @doc "Returns one bounded canonical page after an optional window key."
  @spec page(evaluation(), Key.t() | nil, 1..500) :: {:ok, page()} | {:error, term()}
  def page(evaluation, after_key \\ nil, limit \\ 100)

  def page(%{first_window: first, last_expected_window: last}, after_key, limit)
      when is_integer(limit) and limit in 1..500 do
    with {:ok, start_at} <- page_start(first, last, after_key),
         {:ok, anchors} <- collect(start_at, first.kind, first.timezone, last, limit + 1, []) do
      has_more? = length(anchors) > limit
      items = Enum.take(anchors, limit)

      {:ok,
       %{
         items: items,
         has_more?: has_more?,
         next_after: if(has_more?, do: List.last(items).key, else: nil)
       }}
    end
  end

  def page(_evaluation, _after_key, _limit), do: {:error, :invalid_coverage_page}

  @doc "Returns the hard expected-window safety limit."
  @spec max_windows() :: pos_integer()
  def max_windows, do: @max_windows

  defp last_expected_period(%Effective{through: %TimePeriod{} = period}, _evaluated_at),
    do: {:ok, period}

  defp last_expected_period(%Effective{through: :current} = coverage, evaluated_at),
    do: TimePeriod.current(coverage.kind, evaluated_at, coverage.timezone)

  defp last_expected_period(%Effective{through: :latest_closed} = coverage, evaluated_at) do
    available_at = DateTime.add(evaluated_at, -coverage.availability_delay_seconds, :second)
    TimePeriod.previous_complete(coverage.kind, available_at, coverage.timezone)
  end

  defp anchor(%TimePeriod{} = period),
    do: Anchor.new(period.kind, period.start_at, period.end_at, timezone: period.timezone)

  defp optional_anchor(nil), do: {:ok, nil}
  defp optional_anchor(%TimePeriod{} = period), do: anchor(period)

  defp expected_last_period(%TimePeriod{} = first, %TimePeriod{} = last) do
    if DateTime.compare(last.start_at, first.start_at) == :lt, do: nil, else: last
  end

  defp count(_first, nil), do: {:ok, 0}

  defp count(first, last) do
    if DateTime.compare(last.start_at, first.start_at) == :lt,
      do: {:ok, 0},
      else: count_from(first.start_at, last.start_at, first.kind, 0)
  end

  defp count_from(cursor, last_start, kind, count) do
    if DateTime.compare(cursor, last_start) == :gt do
      {:ok, count}
    else
      next_count = count + 1

      if next_count > @max_windows do
        {:error, :coverage_window_limit_exceeded}
      else
        count_from(TimePeriod.shift!(cursor, kind, 1), last_start, kind, next_count)
      end
    end
  end

  defp page_start(_first, nil, _after_key), do: {:ok, nil}
  defp page_start(first, _last, nil), do: {:ok, first.start_at}

  defp page_start(first, last, after_key) do
    with :ok <- Key.validate(after_key),
         true <- after_key.kind == first.kind and after_key.timezone == first.timezone,
         {:ok, after_start} <- DateTime.from_unix(after_key.start_at_us, :microsecond),
         {:ok, local_after_start} <-
           DateTime.shift_zone(after_start, first.timezone, Favn.Timezone.database!()),
         {:ok, canonical_after_start} <-
           TimePeriod.floor(local_after_start, first.kind, first.timezone),
         true <- DateTime.compare(local_after_start, canonical_after_start) == :eq,
         true <- DateTime.compare(local_after_start, first.start_at) != :lt,
         true <- DateTime.compare(local_after_start, last.start_at) != :gt do
      TimePeriod.shift(local_after_start, first.kind, 1)
    else
      _invalid -> {:error, :coverage_cursor_stale}
    end
  end

  defp collect(nil, _kind, _timezone, _last, _remaining, acc), do: {:ok, Enum.reverse(acc)}
  defp collect(_cursor, _kind, _timezone, _last, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp collect(cursor, kind, timezone, last, remaining, acc) do
    if DateTime.compare(cursor, last.start_at) == :gt do
      {:ok, Enum.reverse(acc)}
    else
      end_at = TimePeriod.shift!(cursor, kind, 1)
      anchor = Anchor.new!(kind, cursor, end_at, timezone: timezone)
      collect(end_at, kind, timezone, last, remaining - 1, [anchor | acc])
    end
  end

  defp checksum(evaluation) do
    payload = %{
      kind: evaluation.coverage.kind,
      timezone: evaluation.coverage.timezone,
      evaluated_at: DateTime.to_iso8601(evaluation.evaluated_at),
      first_window_key: Key.encode(evaluation.first_window.key),
      last_expected_window_key:
        if(evaluation.last_expected_window,
          do: Key.encode(evaluation.last_expected_window.key)
        ),
      expected_count: evaluation.expected_count
    }

    :crypto.hash(:sha256, Serializer.encode_canonical!(payload))
    |> Base.encode16(case: :lower)
  end
end

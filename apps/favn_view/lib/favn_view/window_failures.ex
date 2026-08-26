defmodule FavnView.WindowFailures do
  @moduledoc """
  Groups a backfill's failed coverage windows by the reason they failed.

  A backfill that fails at planning or at child submission fails every window it
  planned, for the same reason, at the same instant. Listing those windows one
  per row says one thing thirty-one times and buries the reason in the
  repetition, so windows are grouped by reason and each group states how many
  windows it covers and the span they run across.

  ## Reading a stored reason

  `last_error` is redacted and flattened to JSON-safe scalars before it is
  stored. A reason that was already a binary survives as itself; anything else is
  `inspect`ed, so a submission that failed with
  `%{reason: :invalid_backfill_pipeline_identity}` arrives here as the string
  `%{"reason" => "invalid_backfill_pipeline_identity"}`.

  `reason/1` therefore unwraps a single-entry inspected map to the value inside
  it, which is the only shape the control plane produces often enough to be worth
  naming. It never parses further: an unrecognised payload is shown as stored,
  because a reason the operator can paste into a search is worth more than a
  tidier one that has lost a term.

  ## What a group cannot say

  Windows that failed *with* a run carry their diagnosis on that run's own page,
  and a group says so by counting how many of its windows are reachable. A group
  with no reachable window is the only account of that failure the product has.
  """

  alias FavnView.WindowLabel

  defmodule Group do
    @moduledoc """
    One reason, and every failed window that recorded it.

    `span` names the coverage the group's windows cover end to end, not the
    windows individually: a reason shared by thirty-one consecutive days is one
    fact about a month, and reading thirty-one labels does not add to it.
    """

    @enforce_keys [:reason, :window_count, :run_count, :span, :attempts]
    defstruct @enforce_keys ++ [:detail, :first_window, :run_ids]

    @type t :: %__MODULE__{
            reason: String.t(),
            detail: String.t() | nil,
            window_count: pos_integer(),
            run_count: non_neg_integer(),
            span: String.t() | nil,
            first_window: String.t() | nil,
            attempts: pos_integer(),
            run_ids: [String.t()]
          }
  end

  @unknown "Reason not recorded"
  @max_run_links 3

  @doc """
  Groups failed windows by reason, worst-covered group first.

  Windows are ordered by how many windows share the reason, so the reason that
  stopped the most coverage is read first. Anything that is not a failed window
  is dropped: the caller reads a status-filtered page, and a stray row from a
  wider read must not be counted as a failure.

  ## Examples

      iex> alias FavnView.WindowFailures
      iex> windows = [
      ...>   %{
      ...>     status: :failed,
      ...>     window_start: ~U[2026-01-01 00:00:00Z],
      ...>     window_end: ~U[2026-01-02 00:00:00Z],
      ...>     run_id: nil,
      ...>     attempt_count: 1,
      ...>     last_error: %{"reason" => "%{\\"reason\\" => \\"bad_identity\\"}"}
      ...>   }
      ...> ]
      iex> [group] = WindowFailures.group(windows, "Etc/UTC")
      iex> {group.reason, group.window_count, group.run_count, group.first_window}
      {"bad_identity", 1, 0, "Jan 1, 2026"}

      iex> alias FavnView.WindowFailures
      iex> WindowFailures.group([], "Etc/UTC")
      []
  """
  @spec group([map()], WindowLabel.configuration()) :: [Group.t()]
  def group(windows, configuration) when is_list(windows) do
    windows
    |> Enum.filter(&failed?/1)
    |> Enum.group_by(&reason/1)
    |> Enum.map(fn {reason, grouped} -> build(reason, grouped, configuration) end)
    |> Enum.sort_by(&{-&1.window_count, &1.reason})
  end

  @doc """
  Returns the readable reason one failed window recorded.

  ## Examples

      iex> alias FavnView.WindowFailures
      iex> WindowFailures.reason(%{last_error: %{"reason" => "window_lease_lost"}})
      "window_lease_lost"

      iex> alias FavnView.WindowFailures
      iex> WindowFailures.reason(%{last_error: %{"reason" => ~S(%{"reason" => "no_runner"})}})
      "no_runner"

      iex> alias FavnView.WindowFailures
      iex> WindowFailures.reason(%{last_error: nil})
      "Reason not recorded"
  """
  @spec reason(map()) :: String.t()
  def reason(window) do
    error = Map.get(window, :last_error) || Map.get(window, "last_error")

    [field(error, "reason"), field(error, "message")]
    |> Enum.find_value(&normalize/1)
    |> Kernel.||(@unknown)
  end

  @doc "The string used when a failed window recorded no reason at all."
  @spec unknown_reason() :: String.t()
  def unknown_reason, do: @unknown

  defp build(reason, windows, configuration) do
    with_runs = Enum.filter(windows, &run_id/1)

    %Group{
      reason: reason,
      detail: detail(windows, reason),
      window_count: length(windows),
      run_count: length(with_runs),
      span: span(windows, configuration),
      first_window: first_window(windows, configuration),
      attempts: windows |> Enum.map(&attempts/1) |> Enum.max(fn -> 1 end),
      run_ids: with_runs |> Enum.map(&run_id/1) |> Enum.take(@max_run_links)
    }
  end

  # The stored message is only worth a second line when it says more than the
  # reason already did. For a reason that was inspected out of a map the two are
  # the same string, and repeating it would make the group look like it held two
  # findings.
  defp detail(windows, reason) do
    windows
    |> Enum.find_value(fn window ->
      error = Map.get(window, :last_error) || Map.get(window, "last_error")
      normalize(field(error, "message"))
    end)
    |> case do
      nil -> nil
      ^reason -> nil
      message -> message
    end
  end

  defp span([_single], _configuration), do: nil

  defp span(windows, configuration) do
    starts = windows |> Enum.map(&start_at/1) |> Enum.reject(&is_nil/1)
    ends = windows |> Enum.map(&end_at/1) |> Enum.reject(&is_nil/1)

    case {starts, ends} do
      {[], _ends} ->
        nil

      {_starts, []} ->
        nil

      {starts, ends} ->
        WindowLabel.compact(Enum.min(starts, DateTime), Enum.max(ends, DateTime), configuration)
    end
  end

  # A group of one has no span to state, so it names its single window instead.
  defp first_window([window], configuration),
    do: WindowLabel.compact(start_at(window), end_at(window), configuration)

  defp first_window(_windows, _configuration), do: nil

  defp failed?(window), do: Map.get(window, :status) == :failed

  defp start_at(window), do: Map.get(window, :window_start)
  defp end_at(window), do: Map.get(window, :window_end)
  defp run_id(window), do: Map.get(window, :run_id)
  defp attempts(window), do: Map.get(window, :attempt_count) || 1

  defp field(error, key) when is_map(error), do: Map.get(error, key)
  defp field(_error, _key), do: nil

  defp normalize(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> unwrap(trimmed)
    end
  end

  defp normalize(_value), do: nil

  # `%{"reason" => "x"}` and `%{reason: :x}` are what an inspected single-key
  # error map looks like once JsonSafe has been through it. Only that one shape
  # is unwrapped; a map with more in it is shown whole, because choosing one of
  # its entries would be choosing what the operator gets to see.
  @single_entry ~r/^%\{(?:"reason"|reason:)\s*(?:=>\s*)?:?"?([A-Za-z0-9_.\-]+)"?\}$/

  defp unwrap(value) do
    case Regex.run(@single_entry, value) do
      [_whole, inner] -> inner
      nil -> value
    end
  end
end

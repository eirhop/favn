defmodule FavnView.RunsFilters do
  @moduledoc """
  The runs list's filter state: URL params in, store filters out.

  An operator arrives at the runs page with one of four questions — what is
  running, what failed today, what ran today, and did this pipeline run every
  day — and each is a pair of a time range and a status. Those two axes are the
  whole filter model, and each is one control: the status is four buttons that
  carry their own counts, the range is a select. Neither writes to the other, so
  every question is one adjustment away from every other.

  Every field round-trips through the URL, so a filtered list is a link an
  operator can send to someone else. Defaults are omitted from the query string,
  which keeps `/runs` meaning "today".

  Times are UTC, because the timestamps the list shows are UTC. "Today" is the
  current UTC day.

  ## Examples

      iex> FavnView.RunsFilters.from_params(%{"status" => "failed"}).status
      :failed

      iex> filters = FavnView.RunsFilters.from_params(%{"range" => "all"})
      iex> FavnView.RunsFilters.window(filters, ~U[2026-07-30 10:00:00Z])
      {nil, nil}

      iex> filters = FavnView.RunsFilters.from_params(%{})
      iex> FavnView.RunsFilters.window(filters, ~U[2026-07-30 10:00:00Z])
      {~U[2026-07-30 00:00:00Z], nil}
  """

  @ranges ~w(hour today week month custom all)a
  @statuses ~w(any active succeeded failed)a
  @orders ~w(started_desc started_asc)a

  @default_range :today
  @default_status :any
  @default_order :started_desc
  @page_size 50

  @status_definitions [
    %{
      status: :active,
      label: "Running",
      hint: "Running or queued",
      icon: "hero-bolt",
      count_key: :active,
      tone: :info
    },
    %{
      status: :failed,
      label: "Failed",
      hint: "Failed runs",
      icon: "hero-exclamation-triangle",
      count_key: :failed,
      tone: :error
    },
    %{
      status: :succeeded,
      label: "Succeeded",
      hint: "Successful runs",
      icon: "hero-check-circle",
      count_key: :succeeded,
      tone: :success
    },
    %{
      status: :any,
      label: "All",
      hint: "Every status",
      icon: "hero-queue-list",
      count_key: :total,
      tone: :neutral
    }
  ]

  defstruct range: @default_range,
            status: @default_status,
            search: "",
            from: nil,
            to: nil,
            order: @default_order,
            after: nil

  @type range :: :hour | :today | :week | :month | :custom | :all
  @type status :: :any | :active | :succeeded | :failed
  @type order :: :started_desc | :started_asc
  @type cursor :: %{started_at: DateTime.t(), root_run_id: String.t()}

  @type t :: %__MODULE__{
          range: range(),
          status: status(),
          search: String.t(),
          from: Date.t() | nil,
          to: Date.t() | nil,
          order: order(),
          after: cursor() | nil
        }

  @doc """
  Reads filter state out of URL params, falling back to the default for anything
  unrecognised.

  A param an operator hand-edited into something invalid must not fail the page,
  so this never raises.

      iex> FavnView.RunsFilters.from_params(%{"range" => "decade"}).range
      :today
  """
  @spec from_params(map()) :: t()
  def from_params(params) when is_map(params) do
    from = date(params["from"])
    to = date(params["to"])

    %__MODULE__{
      range: range(params["range"], from, to),
      status: enum(params["status"], @statuses, @default_status),
      search: search(params["q"]),
      from: from,
      to: to,
      order: enum(params["order"], @orders, @default_order),
      after: cursor(params["after"])
    }
  end

  @doc """
  Writes filter state back to URL params, omitting everything at its default.

      iex> FavnView.RunsFilters.to_params(%FavnView.RunsFilters{})
      []

      iex> FavnView.RunsFilters.to_params(%FavnView.RunsFilters{status: :failed, search: "orders"})
      [{"status", "failed"}, {"q", "orders"}]
  """
  @spec to_params(t()) :: [{String.t(), String.t()}]
  def to_params(%__MODULE__{} = filters) do
    [
      {"range", filters.range, @default_range},
      {"status", filters.status, @default_status},
      {"q", filters.search, ""},
      {"order", filters.order, @default_order}
    ]
    |> Enum.reject(fn {_key, value, default} -> value == default end)
    |> Enum.map(fn {key, value, _default} -> {key, to_string(value)} end)
    |> Kernel.++(date_params(filters))
    |> Kernel.++(cursor_params(filters))
  end

  @doc """
  Merges submitted control values into the current filters.

  Editing a date means the operator wants that date, so it switches the range to
  a custom one even though the range control still reads something else. Any
  change resets the page size: a narrower filter should not inherit however far
  the previous one was scrolled.

  The status is not among these. It is set by the count buttons, which are links,
  so it round-trips through the URL rather than through this form.

  Every change returns to the first page: a narrower filter has no business
  starting where the last one was left.
  """
  @spec change(t(), map()) :: t()
  def change(%__MODULE__{} = filters, params) when is_map(params) do
    current = Map.new(to_params(filters))
    submitted = Map.take(params, ~w(range q from to order))
    merged = Map.merge(current, submitted)

    merged
    |> maybe_force_custom(current, submitted)
    |> Map.delete("after")
    |> from_params()
  end

  @doc "Returns the same filters ordered the other way, from the first page."
  @spec toggle_order(t()) :: t()
  def toggle_order(%__MODULE__{order: :started_desc} = filters),
    do: %{filters | order: :started_asc, after: nil}

  def toggle_order(%__MODULE__{} = filters),
    do: %{filters | order: @default_order, after: nil}

  @doc """
  Returns the same filters starting after one row, which is the next page.

  The row is the last one on screen, so the page after it costs the same read
  whether it is the second page or the two hundredth: the cursor is a bound on the
  index the list is already ordered by, not an offset to count past.
  """
  @spec next_page(t(), DateTime.t() | nil, String.t()) :: t()
  def next_page(%__MODULE__{} = filters, %DateTime{} = started_at, root_run_id)
      when is_binary(root_run_id),
      do: %{filters | after: %{started_at: started_at, root_run_id: root_run_id}}

  def next_page(%__MODULE__{} = filters, _started_at, _root_run_id), do: filters

  @doc "Returns the same filters back at the first page."
  @spec first_page(t()) :: t()
  def first_page(%__MODULE__{} = filters), do: %{filters | after: nil}

  @doc "Whether these filters are showing anything other than the first page."
  @spec paged?(t()) :: boolean()
  def paged?(%__MODULE__{after: nil}), do: false
  def paged?(%__MODULE__{}), do: true

  @doc "How many runs one page holds."
  @spec page_size() :: pos_integer()
  def page_size, do: @page_size

  @doc """
  The instants this range covers, as an inclusive start and an exclusive end.

  `nil` means unbounded on that side.

      iex> filters = FavnView.RunsFilters.from_params(%{"range" => "hour"})
      iex> FavnView.RunsFilters.window(filters, ~U[2026-07-30 10:00:00Z])
      {~U[2026-07-30 09:00:00Z], nil}

      iex> filters = FavnView.RunsFilters.from_params(%{"from" => "2026-07-01", "to" => "2026-07-31"})
      iex> FavnView.RunsFilters.window(filters, ~U[2026-07-30 10:00:00Z])
      {~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z]}
  """
  @spec window(t(), DateTime.t()) :: {DateTime.t() | nil, DateTime.t() | nil}
  def window(%__MODULE__{range: :all}, _now), do: {nil, nil}
  def window(%__MODULE__{range: :hour}, now), do: {DateTime.add(now, -3600, :second), nil}
  def window(%__MODULE__{range: :today}, now), do: {start_of_day(now), nil}
  def window(%__MODULE__{range: :week}, now), do: {days_ago(now, 6), nil}
  def window(%__MODULE__{range: :month}, now), do: {days_ago(now, 29), nil}

  def window(%__MODULE__{range: :custom, from: from, to: to}, _now),
    do: {from && beginning_of_day(from), to && beginning_of_next_day(to)}

  @doc """
  The keyword filters `FavnOrchestrator.page_execution_groups/2` accepts.

      iex> filters = FavnView.RunsFilters.from_params(%{"status" => "active", "range" => "all"})
      iex> FavnView.RunsFilters.store_filters(filters, ~U[2026-07-30 10:00:00Z])
      [status: [:pending, :running], limit: 50, order: :started_desc]
  """
  @spec store_filters(t(), DateTime.t()) :: keyword()
  def store_filters(%__MODULE__{} = filters, now) do
    {started_after, started_before} = window(filters, now)

    [limit: @page_size, order: filters.order]
    |> put_filter(:status, store_status(filters.status))
    |> put_filter(:search, presence(filters.search))
    |> put_filter(:started_after, started_after)
    |> put_filter(:started_before, started_before)
    |> put_filter(:after, filters.after)
  end

  @doc """
  The keyword filters `FavnOrchestrator.count_execution_groups/2` accepts.

  These are `store_filters/2` without the status, the page, or the order, because
  the counts differ from the list in exactly one way: they are taken once per
  status. Deriving them from the same function is what keeps a count equal to the
  number of rows clicking it produces — and a count is of the whole filtered set,
  not of the page being looked at.

      iex> filters = FavnView.RunsFilters.from_params(%{"q" => "orders", "range" => "all"})
      iex> FavnView.RunsFilters.count_filters(filters, ~U[2026-07-30 10:00:00Z])
      [search: "orders"]
  """
  @spec count_filters(t(), DateTime.t()) :: keyword()
  def count_filters(%__MODULE__{} = filters, now),
    do: filters |> store_filters(now) |> Keyword.drop([:status, :limit, :order, :after])

  @doc """
  The status axis, as four choices that each carry their own count.

  `counts` is a `count_filters/2` read, or `nil` before one has been taken. A
  choice with no count still renders — the filter is worth offering whether or
  not its size is known yet.

  Every other axis is preserved, so switching status keeps the range, the dates,
  and the search the operator already set. The page is not an axis: a different
  status starts at its own first page.
  """
  @spec status_filters(t(), map() | nil) :: [map()]
  def status_filters(%__MODULE__{} = filters, counts) do
    Enum.map(@status_definitions, fn choice ->
      %{
        id: choice.status,
        label: choice.label,
        hint: choice.hint,
        icon: choice.icon,
        tone: choice.tone,
        count: counts && Map.get(counts, choice.count_key),
        active?: filters.status == choice.status,
        params: to_params(%{filters | status: choice.status, after: nil})
      }
    end)
  end

  @doc "Whether the range control is showing an explicit pair of dates."
  @spec custom?(t()) :: boolean()
  def custom?(%__MODULE__{range: :custom}), do: true
  def custom?(%__MODULE__{}), do: false

  @doc "Whether anything narrows the list beyond the default view."
  @spec narrowed?(t()) :: boolean()
  def narrowed?(%__MODULE__{} = filters),
    do: filters.status != @default_status or presence(filters.search) != nil

  @doc """
  Whether the search or the time range is off its default.

  On a narrow screen these two live behind a disclosure, and a collapsed control
  that is quietly filtering the list is a control that lies. This is what the
  button uses to say so.

      iex> FavnView.RunsFilters.adjusted?(%FavnView.RunsFilters{})
      false

      iex> FavnView.RunsFilters.adjusted?(%FavnView.RunsFilters{range: :week})
      true
  """
  @spec adjusted?(t()) :: boolean()
  def adjusted?(%__MODULE__{} = filters),
    do: filters.range != @default_range or presence(filters.search) != nil

  @doc "`{label, value}` pairs for the time-range control."
  @spec range_options() :: [{String.t(), String.t()}]
  def range_options do
    [
      {"Last hour", "hour"},
      {"Today", "today"},
      {"Last 7 days", "week"},
      {"Last 30 days", "month"},
      {"Custom range", "custom"},
      {"All time", "all"}
    ]
  end

  @doc "The current range in words, for a heading that has to say what is listed."
  @spec range_label(t()) :: String.t()
  def range_label(%__MODULE__{range: :custom, from: nil, to: nil}), do: "All time"
  def range_label(%__MODULE__{range: :custom, from: from, to: nil}), do: "Since #{from}"
  def range_label(%__MODULE__{range: :custom, from: nil, to: to}), do: "Until #{to}"
  def range_label(%__MODULE__{range: :custom, from: from, to: to}), do: "#{from} to #{to}"

  def range_label(%__MODULE__{range: range}) do
    Enum.find_value(range_options(), "Today", fn {label, value} ->
      value == to_string(range) and label
    end)
  end

  defp store_status(:any), do: nil
  defp store_status(:active), do: [:pending, :running]
  defp store_status(:succeeded), do: :succeeded
  defp store_status(:failed), do: :failed

  defp put_filter(filters, _key, nil), do: filters
  defp put_filter(filters, key, value), do: Keyword.put(filters, key, value)

  defp maybe_force_custom(merged, current, submitted) do
    if edited_date?(current, submitted, "from") or edited_date?(current, submitted, "to") do
      Map.put(merged, "range", "custom")
    else
      merged
    end
  end

  defp edited_date?(current, submitted, key) do
    case Map.fetch(submitted, key) do
      {:ok, value} -> presence(value) != nil and value != Map.get(current, key)
      :error -> false
    end
  end

  defp date_params(%__MODULE__{range: :custom} = filters) do
    [{"from", filters.from}, {"to", filters.to}]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.map(fn {key, value} -> {key, Date.to_iso8601(value)} end)
  end

  defp date_params(%__MODULE__{}), do: []

  # The cursor is the sort key of the last row on the page, which is exactly what
  # the store's keyset needs. It travels in the URL rather than in socket state, so
  # a page is a link and the browser's own history is the way back.
  defp cursor_params(%__MODULE__{after: %{started_at: started_at, root_run_id: run_id}}),
    do: [{"after", "#{DateTime.to_iso8601(started_at)}|#{run_id}"}]

  defp cursor_params(%__MODULE__{}), do: []

  defp cursor(value) when is_binary(value) do
    with [instant, run_id] <- String.split(value, "|", parts: 2),
         {:ok, started_at, _offset} <- DateTime.from_iso8601(instant),
         true <- run_id != "" do
      %{started_at: started_at, root_run_id: run_id}
    else
      _unusable -> nil
    end
  end

  defp cursor(%{started_at: %DateTime{}, root_run_id: run_id} = value) when is_binary(run_id),
    do: value

  defp cursor(_value), do: nil

  defp range(value, from, to) do
    case enum(value, @ranges, nil) do
      nil -> if is_nil(from) and is_nil(to), do: @default_range, else: :custom
      range -> range
    end
  end

  defp enum(nil, _allowed, default), do: default

  defp enum(value, allowed, default) when is_binary(value) do
    Enum.find(allowed, default, &(Atom.to_string(&1) == value))
  end

  defp enum(value, allowed, default), do: if(value in allowed, do: value, else: default)

  defp search(value) when is_binary(value), do: String.trim(value)
  defp search(_value), do: ""

  defp presence(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp presence(_value), do: nil

  defp date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _reason} -> nil
    end
  end

  defp date(%Date{} = value), do: value
  defp date(_value), do: nil

  defp start_of_day(now), do: now |> DateTime.to_date() |> beginning_of_day()

  defp days_ago(now, days), do: now |> start_of_day() |> DateTime.add(-days * 86_400, :second)

  defp beginning_of_day(%Date{} = date), do: DateTime.new!(date, ~T[00:00:00], "Etc/UTC")

  defp beginning_of_next_day(%Date{} = date),
    do: date |> Date.add(1) |> beginning_of_day()
end

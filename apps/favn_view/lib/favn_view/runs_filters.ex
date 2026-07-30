defmodule FavnView.RunsFilters do
  @moduledoc """
  The runs list's filter state: URL params in, store filters out.

  An operator arrives at the runs page with one of four questions — what is
  running, what failed today, what ran today, and did this pipeline run every
  day — and each is a pair of a time range and a status. Those two axes are the
  whole filter model. `presets/2` names the four pairings so the common questions
  are one click, and the axes stay adjustable underneath, so a preset is a
  shortcut rather than a mode the operator can get stuck in.

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
  @default_limit 50
  @limit_step 50
  @max_limit 500

  @preset_definitions [
    %{
      id: :running,
      label: "Running now",
      icon: "hero-bolt",
      range: :all,
      status: :active,
      count_key: :active,
      tone: :info
    },
    %{
      id: :failed_today,
      label: "Failed today",
      icon: "hero-exclamation-triangle",
      range: :today,
      status: :failed,
      count_key: :failed_since,
      tone: :error
    },
    %{
      id: :today,
      label: "Today",
      icon: "hero-calendar-days",
      range: :today,
      status: :any,
      count_key: :started_since,
      tone: :neutral
    },
    %{
      id: :all,
      label: "All runs",
      icon: "hero-clock",
      range: :all,
      status: :any,
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
            limit: @default_limit

  @type range :: :hour | :today | :week | :month | :custom | :all
  @type status :: :any | :active | :succeeded | :failed
  @type order :: :started_desc | :started_asc

  @type t :: %__MODULE__{
          range: range(),
          status: status(),
          search: String.t(),
          from: Date.t() | nil,
          to: Date.t() | nil,
          order: order(),
          limit: pos_integer()
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
      limit: limit(params["limit"])
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
      {"order", filters.order, @default_order},
      {"limit", filters.limit, @default_limit}
    ]
    |> Enum.reject(fn {_key, value, default} -> value == default end)
    |> Enum.map(fn {key, value, _default} -> {key, to_string(value)} end)
    |> Kernel.++(date_params(filters))
  end

  @doc """
  Merges submitted control values into the current filters.

  Editing a date means the operator wants that date, so it switches the range to
  a custom one even though the range control still reads something else. Any
  change resets the page size: a narrower filter should not inherit however far
  the previous one was scrolled.
  """
  @spec change(t(), map()) :: t()
  def change(%__MODULE__{} = filters, params) when is_map(params) do
    current = Map.new(to_params(filters))
    submitted = Map.take(params, ~w(range status q from to order limit))
    merged = Map.merge(current, submitted)

    merged
    |> maybe_force_custom(current, submitted)
    |> from_params()
    |> Map.put(:limit, @default_limit)
  end

  @doc "Returns the same filters ordered the other way."
  @spec toggle_order(t()) :: t()
  def toggle_order(%__MODULE__{order: :started_desc} = filters),
    do: %{filters | order: :started_asc, limit: @default_limit}

  def toggle_order(%__MODULE__{} = filters),
    do: %{filters | order: @default_order, limit: @default_limit}

  @doc "Returns the same filters with room for one more page of rows."
  @spec grow(t()) :: t()
  def grow(%__MODULE__{limit: limit} = filters),
    do: %{filters | limit: min(limit + @limit_step, @max_limit)}

  @doc "How many more rows `grow/1` would ask for, or `nil` at the ceiling."
  @spec growth(t()) :: pos_integer() | nil
  def growth(%__MODULE__{limit: limit}) do
    case min(limit + @limit_step, @max_limit) - limit do
      0 -> nil
      step -> step
    end
  end

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

    [limit: filters.limit, order: filters.order]
    |> put_filter(:status, store_status(filters.status))
    |> put_filter(:search, presence(filters.search))
    |> put_filter(:started_after, started_after)
    |> put_filter(:started_before, started_before)
  end

  @doc """
  The four named questions, each with the params that ask it.

  `counts` is a `FavnOrchestrator.count_execution_groups/2` result, or `nil`
  before one has been read. A preset with no count still renders — the question
  is worth offering whether or not its size is known yet.
  """
  @spec presets(t(), map() | nil) :: [map()]
  def presets(%__MODULE__{} = filters, counts) do
    Enum.map(@preset_definitions, fn preset ->
      target = %{filters | range: preset.range, status: preset.status, from: nil, to: nil}

      %{
        id: preset.id,
        label: preset.label,
        icon: preset.icon,
        tone: preset.tone,
        count: counts && Map.get(counts, preset.count_key),
        active?: filters.range == preset.range and filters.status == preset.status,
        params: to_params(%{target | limit: @default_limit})
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

  @doc "`{label, value}` pairs for the status control."
  @spec status_options() :: [{String.t(), String.t()}]
  def status_options do
    [
      {"Any status", "any"},
      {"Running or queued", "active"},
      {"Succeeded", "succeeded"},
      {"Failed", "failed"}
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

  defp limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> limit(parsed)
      _unparseable -> @default_limit
    end
  end

  defp limit(value) when is_integer(value) and value > 0,
    do: value |> min(@max_limit) |> max(@limit_step)

  defp limit(_value), do: @default_limit

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

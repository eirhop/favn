defmodule Favn.Coverage.Effective do
  @moduledoc """
  Manifest-resolved historical coverage for one asset.

  Boundaries are canonical `Favn.TimePeriod` values in the effective asset
  timezone. Moving end policies remain symbolic until an orchestrator evaluates
  them with an explicit clock.
  """

  alias Favn.Coverage.Spec
  alias Favn.TimePeriod
  alias Favn.Window.Spec, as: WindowSpec

  @type t :: %__MODULE__{
          declared_from: TimePeriod.t(),
          effective_from: TimePeriod.t(),
          through: :latest_closed | :current | TimePeriod.t(),
          availability_delay_seconds: non_neg_integer(),
          kind: TimePeriod.kind(),
          timezone: String.t(),
          timezone_source: WindowSpec.timezone_source(),
          scope_source: :declared | :environment_floor
        }

  @enforce_keys [
    :declared_from,
    :effective_from,
    :through,
    :kind,
    :timezone,
    :timezone_source,
    :scope_source
  ]
  defstruct [
    :declared_from,
    :effective_from,
    :through,
    :kind,
    :timezone,
    :timezone_source,
    :scope_source,
    availability_delay_seconds: 0
  ]

  @doc "Resolves an authored policy against a concrete asset window and optional scope floor."
  @spec resolve(Spec.t() | nil, WindowSpec.t() | nil, Date.t() | nil) ::
          {:ok, t() | nil} | {:error, term()}
  def resolve(nil, _window, _scope_from), do: {:ok, nil}

  def resolve(%Spec{}, nil, _scope_from), do: {:error, :coverage_requires_window}

  def resolve(%Spec{} = spec, %WindowSpec{timezone: timezone} = window, scope_from)
      when is_binary(timezone) do
    with {:ok, spec} <- Spec.validate(spec),
         {:ok, declared_from} <- authored_period(spec.from, window.kind, timezone),
         {:ok, scope_period} <- scope_period(scope_from, window.kind, timezone),
         {effective_from, scope_source} <- effective_from(declared_from, scope_period),
         {:ok, through} <- authored_through(spec.through, window.kind, timezone),
         :ok <- validate_period_order(declared_from, through) do
      {:ok,
       %__MODULE__{
         declared_from: declared_from,
         effective_from: effective_from,
         through: through,
         availability_delay_seconds: spec.availability_delay_seconds,
         kind: window.kind,
         timezone: timezone,
         timezone_source: window.timezone_source,
         scope_source: scope_source
       }}
    end
  end

  def resolve(%Spec{}, %WindowSpec{}, _scope_from), do: {:error, :unresolved_window_timezone}

  @doc "Rehydrates a manifest-shaped effective coverage value."
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = value), do: validate(value)

  def from_value(value) when is_map(value) do
    with :ok <- reject_unknown_fields(value),
         {:ok, declared_from} <- period_from_value(field_value(value, :declared_from)),
         {:ok, effective_from} <- period_from_value(field_value(value, :effective_from)),
         {:ok, through} <- through_from_value(field_value(value, :through)),
         {:ok, kind} <- decode_kind(field_value(value, :kind)),
         {:ok, timezone_source} <- decode_timezone_source(field_value(value, :timezone_source)),
         {:ok, scope_source} <- decode_scope_source(field_value(value, :scope_source)) do
      value = %__MODULE__{
        declared_from: declared_from,
        effective_from: effective_from,
        through: through,
        availability_delay_seconds: field_value(value, :availability_delay_seconds, 0),
        kind: kind,
        timezone: field_value(value, :timezone),
        timezone_source: timezone_source,
        scope_source: scope_source
      }

      validate(value)
    end
  end

  def from_value(value), do: {:error, {:invalid_effective_coverage, value}}

  @doc "Validates a resolved coverage value."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = value) do
    with :ok <- validate_kind(value.kind),
         :ok <- Favn.Window.Validate.timezone(value.timezone),
         :ok <- validate_timezone_source(value.timezone_source),
         :ok <- validate_period(:declared_from, value.declared_from, value),
         :ok <- validate_period(:effective_from, value.effective_from, value),
         :ok <- validate_through(value.through, value),
         :ok <- validate_delay(value.availability_delay_seconds, value.through),
         :ok <- validate_scope_resolution(value),
         :ok <- validate_period_order(value.declared_from, value.through) do
      {:ok, value}
    end
  end

  defp authored_period(%Date{} = date, :hour, _timezone),
    do: {:error, {:hourly_coverage_requires_datetime, date}}

  defp authored_period(%Date{} = date, kind, timezone), do: date_period(date, kind, timezone)

  defp authored_period(%DateTime{} = datetime, kind, timezone),
    do: datetime_period(datetime, kind, timezone)

  defp authored_through(value, _kind, _timezone) when value in [:latest_closed, :current],
    do: {:ok, value}

  defp authored_through(value, kind, timezone), do: authored_period(value, kind, timezone)

  defp scope_period(nil, _kind, _timezone), do: {:ok, nil}
  defp scope_period(%Date{} = date, kind, timezone), do: date_period(date, kind, timezone)

  defp date_period(%Date{} = date, :hour, timezone),
    do: TimePeriod.bounds(:hour, Date.to_iso8601(date) <> "T00", timezone)

  defp date_period(%Date{} = date, :day, timezone),
    do: TimePeriod.bounds(:day, Date.to_iso8601(date), timezone)

  defp date_period(%Date{} = date, :month, timezone) do
    TimePeriod.bounds(:month, Calendar.strftime(date, "%Y-%m"), timezone)
  end

  defp date_period(%Date{} = date, :year, timezone),
    do: TimePeriod.bounds(:year, Integer.to_string(date.year), timezone)

  defp datetime_period(%DateTime{} = datetime, kind, timezone),
    do: TimePeriod.current(kind, datetime, timezone)

  defp effective_from(declared, nil), do: {declared, :declared}

  defp effective_from(%TimePeriod{} = declared, %TimePeriod{} = scope) do
    case DateTime.compare(scope.start_at, declared.start_at) do
      :gt -> {scope, :environment_floor}
      _other -> {declared, :declared}
    end
  end

  defp validate_period_order(_from, through) when through in [:latest_closed, :current], do: :ok

  defp validate_period_order(%TimePeriod{} = from, %TimePeriod{} = through) do
    if DateTime.compare(through.start_at, from.start_at) == :lt,
      do: {:error, {:coverage_through_before_from, from, through}},
      else: :ok
  end

  defp period_from_value(%TimePeriod{} = period), do: {:ok, period}

  defp period_from_value(value) when is_map(value) do
    with {:ok, kind} <- decode_kind(field_value(value, :kind)),
         timezone when is_binary(timezone) <- field_value(value, :timezone),
         {:ok, start_at} <- decode_datetime(field_value(value, :start_at), timezone),
         {:ok, end_at} <- decode_datetime(field_value(value, :end_at), timezone) do
      {:ok, %TimePeriod{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}}
    else
      _invalid -> {:error, {:invalid_coverage_period, value}}
    end
  end

  defp period_from_value(value), do: {:error, {:invalid_coverage_period, value}}

  defp through_from_value(value) when value in [:latest_closed, :current], do: {:ok, value}
  defp through_from_value("latest_closed"), do: {:ok, :latest_closed}
  defp through_from_value("current"), do: {:ok, :current}
  defp through_from_value(value), do: period_from_value(value)

  defp decode_kind(value) when value in [:hour, :day, :month, :year], do: {:ok, value}
  defp decode_kind("hour"), do: {:ok, :hour}
  defp decode_kind("day"), do: {:ok, :day}
  defp decode_kind("month"), do: {:ok, :month}
  defp decode_kind("year"), do: {:ok, :year}
  defp decode_kind(value), do: {:error, {:invalid_coverage_kind, value}}

  defp decode_timezone_source(value)
       when value in [:local, :namespace, :application_default, :utc_fallback],
       do: {:ok, value}

  defp decode_timezone_source("local"), do: {:ok, :local}
  defp decode_timezone_source("namespace"), do: {:ok, :namespace}
  defp decode_timezone_source("application_default"), do: {:ok, :application_default}
  defp decode_timezone_source("utc_fallback"), do: {:ok, :utc_fallback}
  defp decode_timezone_source(value), do: {:error, {:invalid_coverage_timezone_source, value}}

  defp decode_scope_source(value) when value in [:declared, :environment_floor], do: {:ok, value}
  defp decode_scope_source("declared"), do: {:ok, :declared}
  defp decode_scope_source("environment_floor"), do: {:ok, :environment_floor}
  defp decode_scope_source(value), do: {:error, {:invalid_coverage_scope_source, value}}

  defp decode_datetime(%DateTime{} = datetime, timezone),
    do: DateTime.shift_zone(datetime, timezone, Favn.Timezone.database!())

  defp decode_datetime(value, timezone) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        DateTime.shift_zone(datetime, timezone, Favn.Timezone.database!())

      {:error, reason} ->
        {:error, {:invalid_coverage_datetime, value, reason}}
    end
  end

  defp decode_datetime(value, _timezone), do: {:error, {:invalid_coverage_datetime, value}}

  defp validate_kind(kind) when kind in [:hour, :day, :month, :year], do: :ok
  defp validate_kind(kind), do: {:error, {:invalid_coverage_kind, kind}}

  defp validate_timezone_source(source)
       when source in [:local, :namespace, :application_default, :utc_fallback],
       do: :ok

  defp validate_timezone_source(source),
    do: {:error, {:invalid_coverage_timezone_source, source}}

  defp validate_period(field, %TimePeriod{} = period, coverage) do
    expected_start = TimePeriod.floor!(period.start_at, coverage.kind, coverage.timezone)
    expected_end = TimePeriod.shift!(period.start_at, coverage.kind, 1)

    cond do
      period.kind != coverage.kind ->
        {:error, {:coverage_period_kind_mismatch, field, period.kind, coverage.kind}}

      period.timezone != coverage.timezone or period.start_at.time_zone != coverage.timezone or
          period.end_at.time_zone != coverage.timezone ->
        {:error, {:coverage_period_timezone_mismatch, field, period.timezone, coverage.timezone}}

      DateTime.compare(period.start_at, expected_start) != :eq ->
        {:error, {:noncanonical_coverage_period_start, field, period.start_at}}

      DateTime.compare(period.end_at, expected_end) != :eq ->
        {:error, {:invalid_coverage_period_bounds, field, period}}

      true ->
        :ok
    end
  rescue
    _error -> {:error, {:invalid_coverage_period, field, period}}
  end

  defp validate_period(field, period, _coverage),
    do: {:error, {:invalid_coverage_period, field, period}}

  defp validate_through(value, _coverage) when value in [:latest_closed, :current], do: :ok

  defp validate_through(%TimePeriod{} = period, coverage),
    do: validate_period(:through, period, coverage)

  defp validate_through(value, _coverage), do: {:error, {:invalid_coverage_through, value}}

  defp validate_delay(delay, through) when is_integer(delay) and delay >= 0 do
    if through == :latest_closed or delay == 0,
      do: :ok,
      else: {:error, {:coverage_delay_requires_latest_closed, through}}
  end

  defp validate_delay(delay, _through),
    do: {:error, {:invalid_coverage_delay_seconds, delay}}

  defp validate_scope_resolution(%__MODULE__{} = coverage) do
    cond do
      coverage.scope_source not in [:declared, :environment_floor] ->
        {:error, {:invalid_coverage_scope_source, coverage.scope_source}}

      DateTime.compare(coverage.effective_from.start_at, coverage.declared_from.start_at) == :lt ->
        {:error, :invalid_effective_coverage_boundary}

      coverage.scope_source == :declared and coverage.effective_from != coverage.declared_from ->
        {:error, :invalid_effective_coverage_boundary}

      true ->
        :ok
    end
  end

  @persisted_fields [
    :declared_from,
    :effective_from,
    :through,
    :availability_delay_seconds,
    :kind,
    :timezone,
    :timezone_source,
    :scope_source,
    "declared_from",
    "effective_from",
    "through",
    "availability_delay_seconds",
    "kind",
    "timezone",
    "timezone_source",
    "scope_source"
  ]

  defp reject_unknown_fields(value) do
    unknown =
      value
      |> Map.keys()
      |> Enum.reject(&(&1 in @persisted_fields))
      |> Enum.sort_by(&inspect/1)

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_effective_coverage_fields, unknown}}
  end

  defp field_value(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

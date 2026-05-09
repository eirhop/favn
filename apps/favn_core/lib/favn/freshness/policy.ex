defmodule Favn.Freshness.Policy do
  @moduledoc """
  Normalized freshness policy domain value.

  Freshness policies describe how runtime layers decide whether a previous asset
  success is fresh enough to satisfy a run. Authors normally provide these values
  through `@freshness` in `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, or
  `Favn.Assets`. The compiler stores the normalized policy in the manifest.

  Supported policy modes are:

  ## Accepted V1 Input Values

  - `nil`: no freshness policy.
  - `:daily` or `:day`: one successful run per local calendar day in `"Etc/UTC"`.
  - `{:daily, timezone: "Europe/Oslo"}` or `{:day, timezone: "Europe/Oslo"}`:
    daily calendar freshness in the supplied IANA timezone.
  - `[max_age: {:hours, 6}]`: rolling max-age freshness. The tuple is
    `{unit, amount}` and accepts `:second`, `:minute`, `:hour`, `:day` plus plural
    aliases.
  - `[window_success: true]`: freshness is scoped to the exact runtime window.
  - `:always`: always run when planned, overriding the implicit window-success
    default for windowed assets.
  - `%Favn.Freshness.Policy{}` or JSON/manifest-shaped maps with `:mode`/`"mode"`.

  ## Runtime Behavior

  Under the default `:auto` refresh policy, the orchestrator skips nodes whose
  stored freshness state satisfies the policy, runs nodes whose freshness is
  missing or expired, and marks downstream nodes stale only when an upstream node
  actually refreshed in the same run. Backfill child runs default to
  `refresh: :missing`, which skips prior successes even for `:always` assets.

  Read `Favn.Freshness.Key` for the stable keys used to persist freshness state.

  """

  alias Favn.Window.Validate

  @typedoc "Calendar period kinds supported by freshness policies."
  @type calendar_kind :: :day

  @typedoc "Units supported by max-age freshness policies."
  @type max_age_unit :: :second | :minute | :hour | :day

  @typep max_age_input_unit :: max_age_unit() | :seconds | :minutes | :hours | :days

  @typedoc "Freshness policy mode."
  @type mode :: :calendar_period | :max_age | :window_success | :always

  @typedoc "Normalized freshness policy value."
  @type t :: %__MODULE__{
          mode: mode(),
          kind: calendar_kind() | nil,
          timezone: String.t() | nil,
          amount: pos_integer() | nil,
          unit: max_age_unit() | nil
        }

  defstruct [:mode, :kind, :timezone, :amount, :unit]

  @doc """
  Normalizes a V1 freshness policy value.

  Returns `{:ok, nil}` for a missing policy. The `:always` policy is represented
  as its own explicit mode so later integration can distinguish it from a missing
  policy and let it override window defaults.

  ## Examples

      iex> Favn.Freshness.Policy.from_value(:daily)
      {:ok, %Favn.Freshness.Policy{mode: :calendar_period, kind: :day, timezone: "Etc/UTC"}}

      iex> Favn.Freshness.Policy.from_value({:daily, timezone: "Europe/Oslo"})
      {:ok, %Favn.Freshness.Policy{mode: :calendar_period, kind: :day, timezone: "Europe/Oslo"}}

      iex> Favn.Freshness.Policy.from_value(max_age: {:hours, 24})
      {:ok, %Favn.Freshness.Policy{mode: :max_age, amount: 24, unit: :hour}}

      iex> Favn.Freshness.Policy.from_value(window_success: true)
      {:ok, %Favn.Freshness.Policy{mode: :window_success}}

      iex> Favn.Freshness.Policy.from_value(:always)
      {:ok, %Favn.Freshness.Policy{mode: :always}}

  """
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = policy), do: validate(policy)
  def from_value(:daily), do: calendar(:day)
  def from_value(:day), do: calendar(:day)
  def from_value(:always), do: always()

  def from_value({kind, opts}) when kind in [:daily, :day] and is_list(opts) do
    calendar(:day, opts)
  end

  def from_value(value) when is_list(value) do
    if Keyword.keyword?(value) do
      from_keyword(value)
    else
      {:error, {:invalid_freshness_policy, value}}
    end
  end

  def from_value(value) when is_map(value) do
    case normalize_mode(field_value(value, :mode)) do
      :calendar_period ->
        calendar(normalize_calendar_kind(field_value(value, :kind)),
          timezone: field_value(value, :timezone)
        )

      :max_age ->
        max_age(
          field_value(value, :amount),
          normalize_max_age_unit_input(field_value(value, :unit))
        )

      :window_success ->
        window_success()

      :always ->
        always()

      {:error, mode} ->
        {:error, {:invalid_freshness_policy_mode, mode}}
    end
  end

  def from_value(value), do: {:error, {:invalid_freshness_policy, value}}

  @doc """
  Normalizes a V1 freshness policy value or raises `ArgumentError`.
  """
  @spec from_value!(term()) :: t() | nil
  def from_value!(value) do
    case from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid freshness policy: #{inspect(reason)}"
    end
  end

  @doc """
  Validates a normalized freshness policy struct.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{mode: :calendar_period, kind: kind, timezone: timezone} = policy) do
    with :ok <- validate_calendar_kind(kind),
         :ok <- Validate.timezone(timezone) do
      {:ok, %{policy | amount: nil, unit: nil}}
    end
  end

  def validate(%__MODULE__{mode: :max_age, amount: amount, unit: unit} = policy) do
    with :ok <- validate_amount(amount),
         :ok <- validate_max_age_unit(unit) do
      {:ok, %{policy | kind: nil, timezone: nil}}
    end
  end

  def validate(%__MODULE__{mode: :window_success} = policy) do
    {:ok, %{policy | kind: nil, timezone: nil, amount: nil, unit: nil}}
  end

  def validate(%__MODULE__{mode: :always} = policy) do
    {:ok, %{policy | kind: nil, timezone: nil, amount: nil, unit: nil}}
  end

  def validate(%__MODULE__{mode: mode}), do: {:error, {:invalid_freshness_policy_mode, mode}}

  @doc """
  Builds a calendar-period freshness policy.
  """
  @spec calendar(calendar_kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def calendar(kind, opts \\ []) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         :ok <- validate_calendar_kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- Validate.timezone(timezone) do
      {:ok, %__MODULE__{mode: :calendar_period, kind: kind, timezone: timezone}}
    end
  end

  @doc """
  Builds a max-age freshness policy.

  Accepts canonical singular units and V1 plural unit aliases.
  """
  @spec max_age(pos_integer(), max_age_input_unit()) :: {:ok, t()} | {:error, term()}
  def max_age(amount, unit) do
    with :ok <- validate_amount(amount),
         {:ok, normalized_unit} <- normalize_max_age_unit(unit) do
      {:ok, %__MODULE__{mode: :max_age, amount: amount, unit: normalized_unit}}
    end
  end

  @doc """
  Builds a policy that derives freshness from the relevant window success.
  """
  @spec window_success() :: {:ok, t()}
  def window_success, do: {:ok, %__MODULE__{mode: :window_success}}

  @doc """
  Builds a policy that explicitly makes the asset run whenever planned.
  """
  @spec always() :: {:ok, t()}
  def always, do: {:ok, %__MODULE__{mode: :always}}

  defp from_keyword(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:max_age, :window_success]) do
      case opts do
        [max_age: {unit, amount}] -> max_age(amount, unit)
        [window_success: true] -> window_success()
        _other -> {:error, {:invalid_freshness_policy, opts}}
      end
    end
  end

  defp validate_calendar_kind(:day), do: :ok
  defp validate_calendar_kind(kind), do: {:error, {:invalid_freshness_calendar_kind, kind}}

  defp validate_amount(amount) when is_integer(amount) and amount > 0, do: :ok
  defp validate_amount(amount), do: {:error, {:invalid_freshness_max_age_amount, amount}}

  defp normalize_max_age_unit(unit) when unit in [:second, :seconds], do: {:ok, :second}
  defp normalize_max_age_unit(unit) when unit in [:minute, :minutes], do: {:ok, :minute}
  defp normalize_max_age_unit(unit) when unit in [:hour, :hours], do: {:ok, :hour}
  defp normalize_max_age_unit(unit) when unit in [:day, :days], do: {:ok, :day}
  defp normalize_max_age_unit(unit), do: {:error, {:invalid_freshness_max_age_unit, unit}}

  defp normalize_mode(mode) when mode in [:calendar_period, :max_age, :window_success, :always],
    do: mode

  defp normalize_mode("calendar_period"), do: :calendar_period
  defp normalize_mode("max_age"), do: :max_age
  defp normalize_mode("window_success"), do: :window_success
  defp normalize_mode("always"), do: :always
  defp normalize_mode(mode), do: {:error, mode}

  defp normalize_calendar_kind(kind) when kind in [:daily, :day, "daily", "day"], do: :day
  defp normalize_calendar_kind(kind), do: kind

  defp normalize_max_age_unit_input("second"), do: :second
  defp normalize_max_age_unit_input("seconds"), do: :seconds
  defp normalize_max_age_unit_input("minute"), do: :minute
  defp normalize_max_age_unit_input("minutes"), do: :minutes
  defp normalize_max_age_unit_input("hour"), do: :hour
  defp normalize_max_age_unit_input("hours"), do: :hours
  defp normalize_max_age_unit_input("day"), do: :day
  defp normalize_max_age_unit_input("days"), do: :days
  defp normalize_max_age_unit_input(unit), do: unit

  defp validate_max_age_unit(unit) do
    case normalize_max_age_unit(unit) do
      {:ok, ^unit} -> :ok
      {:ok, _normalized_unit} -> {:error, {:invalid_freshness_max_age_unit, unit}}
      {:error, _reason} = error -> error
    end
  end

  defp field_value(value, field) when is_map(value) do
    Map.get(value, field, Map.get(value, Atom.to_string(field)))
  end
end

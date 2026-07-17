defmodule Favn.Window.Spec do
  @moduledoc """
  Canonical asset-level runtime window specification.

  A window spec describes how an asset is windowed independently from any
  particular run request.

  `lookback` adds earlier data windows to every resolved run anchor.
  `refresh_from` gives each exact data window a repeatable calendar refresh
  cadence under window-success freshness. For example, a monthly asset with
  `lookback: 1, refresh_from: :day` plans the anchor month plus the previous
  month and tracks a separate daily success for each. A success for one month
  never satisfies the other.

  When `refresh_from` is nil, window-success freshness is keyed only by the exact
  runtime window. An explicit non-window freshness policy such as
  `freshness :daily` remains asset-wide and overrides the implicit
  window-success policy.
  """

  alias Favn.Window.Policy
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month | :year
  @type refresh_from :: :hour | :day | :month | :year

  @type t :: %__MODULE__{
          kind: kind(),
          lookback: non_neg_integer(),
          refresh_from: refresh_from() | nil,
          required: boolean(),
          timezone: String.t()
        }

  defstruct [:kind, lookback: 0, refresh_from: nil, required: false, timezone: "Etc/UTC"]

  @doc """
  Build and validate a canonical `%Favn.Window.Spec{}`.

  ## Examples

      iex> Favn.Window.Spec.new(:day)
      {:ok, %Favn.Window.Spec{kind: :day, lookback: 0, refresh_from: nil, required: false, timezone: "Etc/UTC"}}

      iex> Favn.Window.Spec.new(:month, lookback: 2, refresh_from: :day)
      {:ok, %Favn.Window.Spec{kind: :month, lookback: 2, refresh_from: :day, required: false, timezone: "Etc/UTC"}}
  """
  @spec new(kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(kind, opts \\ []) when is_list(opts) do
    with :ok <-
           Validate.strict_keyword_opts(opts, [:lookback, :refresh_from, :required, :timezone]),
         :ok <- Validate.kind(kind),
         lookback <- Keyword.get(opts, :lookback, 0),
         refresh_from <- Keyword.get(opts, :refresh_from),
         required <- Keyword.get(opts, :required, false),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- validate_lookback(lookback),
         :ok <- validate_refresh_from(kind, refresh_from),
         :ok <- validate_required(required),
         :ok <- Validate.timezone(timezone) do
      {:ok,
       %__MODULE__{
         kind: kind,
         lookback: lookback,
         refresh_from: refresh_from,
         required: required,
         timezone: timezone
       }}
    end
  end

  @doc """
  Build and validate a canonical `%Favn.Window.Spec{}`.

  Raises `ArgumentError` on invalid input.
  """
  @spec new!(kind(), keyword()) :: t()
  def new!(kind, opts \\ []) when is_list(opts) do
    case new(kind, opts) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid window spec: #{inspect(reason)}"
    end
  end

  @doc """
  Normalizes persisted or DSL-shaped values into an asset runtime window spec.

  This accepts the canonical struct, atom/string kind shorthands, persisted maps,
  and policy-shaped values used by older manifests. Nil and empty option values
  are omitted so normal `new/2` defaults still apply.

  ## Examples

      iex> Favn.Window.Spec.from_value(%{"kind" => "month", "refresh_from" => "day"})
      {:ok, %Favn.Window.Spec{kind: :month, lookback: 0, refresh_from: :day, required: false, timezone: "Etc/UTC"}}

      iex> Favn.Window.Spec.from_value(Favn.Window.Policy.new!(:daily))
      {:ok, %Favn.Window.Spec{kind: :day, lookback: 0, refresh_from: nil, required: false, timezone: "Etc/UTC"}}
  """
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}

  def from_value(%__MODULE__{} = spec) do
    with :ok <- validate(spec), do: {:ok, spec}
  end

  def from_value(%Policy{kind: kind, timezone: timezone}) do
    opts = [] |> maybe_put(:timezone, timezone)
    from_kind_and_opts(kind, opts)
  end

  def from_value(kind) when is_atom(kind) or is_binary(kind), do: from_kind_and_opts(kind, [])

  def from_value(value) when is_map(value) do
    kind = field_value(value, :kind)

    with {:ok, refresh_from} <- value |> field_value(:refresh_from) |> normalize_optional_kind() do
      opts =
        []
        |> maybe_put(:lookback, field_value(value, :lookback))
        |> maybe_put(:required, field_value(value, :required))
        |> maybe_put(:refresh_from, refresh_from)
        |> maybe_put(:timezone, field_value(value, :timezone))

      from_kind_and_opts(kind, opts)
    end
  end

  def from_value(value) when is_list(value) do
    if Keyword.keyword?(value) do
      value |> Map.new() |> from_value()
    else
      {:error, {:invalid_window_spec, value}}
    end
  end

  def from_value(value), do: {:error, {:invalid_window_spec, value}}

  @doc """
  Validate an existing `%Favn.Window.Spec{}` struct.
  """
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = spec) do
    with :ok <- Validate.kind(spec.kind),
         :ok <- validate_lookback(spec.lookback),
         :ok <- validate_refresh_from(spec.kind, spec.refresh_from),
         :ok <- validate_required(spec.required) do
      Validate.timezone(spec.timezone)
    end
  end

  defp from_kind_and_opts(kind, opts) do
    with {:ok, kind} <- normalize_kind(kind) do
      new(kind, opts)
    end
  end

  defp normalize_optional_kind(nil), do: {:ok, nil}
  defp normalize_optional_kind(""), do: {:ok, nil}
  defp normalize_optional_kind(kind), do: normalize_kind(kind)

  defp normalize_kind(kind) when kind in [:hour, :hourly], do: {:ok, :hour}
  defp normalize_kind(kind) when kind in [:day, :daily], do: {:ok, :day}
  defp normalize_kind(kind) when kind in [:month, :monthly], do: {:ok, :month}
  defp normalize_kind(kind) when kind in [:year, :yearly], do: {:ok, :year}
  defp normalize_kind(kind) when kind in ["hour", "hourly"], do: {:ok, :hour}
  defp normalize_kind(kind) when kind in ["day", "daily"], do: {:ok, :day}
  defp normalize_kind(kind) when kind in ["month", "monthly"], do: {:ok, :month}
  defp normalize_kind(kind) when kind in ["year", "yearly"], do: {:ok, :year}
  defp normalize_kind(kind), do: {:error, {:invalid_window_spec_kind, kind}}

  defp field_value(map, key) do
    string_key = Atom.to_string(key)

    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Map.get(map, string_key)
    end
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, _key, ""), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp validate_lookback(lookback) when is_integer(lookback) and lookback >= 0, do: :ok
  defp validate_lookback(lookback), do: {:error, {:invalid_lookback, lookback}}

  defp validate_required(value) when is_boolean(value), do: :ok
  defp validate_required(value), do: {:error, {:invalid_required, value}}

  defp validate_refresh_from(:hour, nil), do: :ok
  defp validate_refresh_from(:hour, :hour), do: :ok
  defp validate_refresh_from(:hour, value), do: {:error, {:invalid_refresh_from, :hour, value}}

  defp validate_refresh_from(:day, nil), do: :ok
  defp validate_refresh_from(:day, :hour), do: :ok
  defp validate_refresh_from(:day, :day), do: :ok
  defp validate_refresh_from(:day, value), do: {:error, {:invalid_refresh_from, :day, value}}

  defp validate_refresh_from(:month, nil), do: :ok
  defp validate_refresh_from(:month, :day), do: :ok
  defp validate_refresh_from(:month, :month), do: :ok
  defp validate_refresh_from(:month, value), do: {:error, {:invalid_refresh_from, :month, value}}

  defp validate_refresh_from(:year, nil), do: :ok
  defp validate_refresh_from(:year, :month), do: :ok
  defp validate_refresh_from(:year, :year), do: :ok
  defp validate_refresh_from(:year, value), do: {:error, {:invalid_refresh_from, :year, value}}
end

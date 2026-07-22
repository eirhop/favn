defmodule Favn.Window.Spec do
  @moduledoc """
  Canonical asset-level runtime window specification.

  A window spec describes how an asset is windowed independently from any
  particular run request.

  `refresh_from` gives each exact data window a repeatable calendar refresh
  cadence under window-success freshness. Operational lookback is a pipeline
  invocation policy and belongs to `Favn.Window.Policy`, not this asset-level
  identity.

  When `refresh_from` is nil, window-success freshness is keyed only by the exact
  runtime window. An explicit non-window freshness policy such as
  `freshness :daily` remains asset-wide and overrides the implicit
  window-success policy.
  """

  alias Favn.Window.Validate

  @persisted_fields [
    :kind,
    :refresh_from,
    :required,
    :timezone,
    :timezone_source,
    "kind",
    "refresh_from",
    "required",
    "timezone",
    "timezone_source"
  ]

  @type kind :: :hour | :day | :month | :year
  @type refresh_from :: :hour | :day | :month | :year
  @type timezone_source :: :local | :namespace | :application_default | :utc_fallback | nil

  @type t :: %__MODULE__{
          kind: kind(),
          refresh_from: refresh_from() | nil,
          required: boolean(),
          timezone: String.t() | nil,
          timezone_source: timezone_source()
        }

  defstruct [:kind, :timezone, :timezone_source, refresh_from: nil, required: false]

  @doc """
  Build and validate a canonical `%Favn.Window.Spec{}`.

  ## Examples

      iex> Favn.Window.Spec.new(:day)
      {:ok, %Favn.Window.Spec{kind: :day, refresh_from: nil, required: false, timezone: nil, timezone_source: nil}}

      iex> Favn.Window.Spec.new(:month, refresh_from: :day)
      {:ok, %Favn.Window.Spec{kind: :month, refresh_from: :day, required: false, timezone: nil, timezone_source: nil}}
  """
  @spec new(kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(kind, opts \\ []) when is_list(opts) do
    with :ok <-
           Validate.strict_keyword_opts(opts, [:refresh_from, :required, :timezone]),
         :ok <- Validate.kind(kind),
         refresh_from <- Keyword.get(opts, :refresh_from),
         required <- Keyword.get(opts, :required, false),
         timezone <- Keyword.get(opts, :timezone),
         :ok <- validate_refresh_from(kind, refresh_from),
         :ok <- validate_required(required),
         :ok <- validate_optional_timezone(timezone) do
      {:ok,
       %__MODULE__{
         kind: kind,
         refresh_from: refresh_from,
         required: required,
         timezone: timezone,
         timezone_source: if(is_binary(timezone), do: :local)
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

  This accepts the canonical struct, atom/string kind shorthands, and schema 11
  persisted maps. Nil and empty option values are omitted so normal `new/2`
  defaults still apply. Pipeline policy fields such as `lookback` are rejected.

  ## Examples

      iex> Favn.Window.Spec.from_value(%{"kind" => "month", "refresh_from" => "day"})
      {:ok, %Favn.Window.Spec{kind: :month, refresh_from: :day, required: false, timezone: nil, timezone_source: nil}}

      iex> Favn.Window.Spec.from_value(%{"kind" => "day", "lookback" => 1})
      {:error, {:unknown_window_spec_fields, ["lookback"]}}
  """
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}

  def from_value(%__MODULE__{} = spec) do
    with :ok <- validate(spec), do: {:ok, spec}
  end

  def from_value(kind) when is_atom(kind) or is_binary(kind), do: from_kind_and_opts(kind, [])

  def from_value(value) when is_map(value) do
    kind = field_value(value, :kind)

    with :ok <- reject_unknown_fields(value),
         {:ok, refresh_from} <- value |> field_value(:refresh_from) |> normalize_optional_kind() do
      opts =
        []
        |> maybe_put(:required, field_value(value, :required))
        |> maybe_put(:refresh_from, refresh_from)
        |> maybe_put(:timezone, field_value(value, :timezone))

      with {:ok, spec} <- from_kind_and_opts(kind, opts),
           {:ok, source} <- normalize_timezone_source(field_value(value, :timezone_source)) do
        {:ok, %{spec | timezone_source: source || spec.timezone_source}}
      end
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
         :ok <- validate_refresh_from(spec.kind, spec.refresh_from),
         :ok <- validate_required(spec.required),
         :ok <- validate_optional_timezone(spec.timezone) do
      validate_timezone_source(spec.timezone, spec.timezone_source)
    end
  end

  @doc false
  @spec with_declaration_source(t(), :local | :namespace) :: t()
  def with_declaration_source(%__MODULE__{timezone: timezone} = spec, source)
      when source in [:local, :namespace] do
    %{spec | timezone_source: if(is_binary(timezone), do: source)}
  end

  @doc false
  @spec resolve_timezone(t(), String.t(), :application_default | :utc_fallback) ::
          {:ok, t()} | {:error, term()}
  def resolve_timezone(%__MODULE__{timezone: timezone} = spec, default, default_source) do
    effective = timezone || default
    source = spec.timezone_source || default_source

    with :ok <- Validate.timezone(effective),
         :ok <- validate_timezone_source(effective, source) do
      {:ok, %{spec | timezone: effective, timezone_source: source}}
    end
  end

  defp from_kind_and_opts(kind, opts) do
    with {:ok, kind} <- normalize_kind(kind) do
      new(kind, opts)
    end
  end

  defp reject_unknown_fields(value) do
    unknown =
      value
      |> Map.keys()
      |> Enum.reject(&(&1 in @persisted_fields))
      |> Enum.sort_by(&inspect/1)

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_window_spec_fields, unknown}}
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

  defp validate_optional_timezone(nil), do: :ok
  defp validate_optional_timezone(timezone), do: Validate.timezone(timezone)

  defp validate_timezone_source(nil, nil), do: :ok

  defp validate_timezone_source(timezone, source)
       when is_binary(timezone) and
              source in [:local, :namespace, :application_default, :utc_fallback],
       do: :ok

  defp validate_timezone_source(_timezone, source),
    do: {:error, {:invalid_window_timezone_source, source}}

  defp normalize_timezone_source(nil), do: {:ok, nil}

  defp normalize_timezone_source(source)
       when source in [:local, :namespace, :application_default, :utc_fallback],
       do: {:ok, source}

  defp normalize_timezone_source("local"), do: {:ok, :local}
  defp normalize_timezone_source("namespace"), do: {:ok, :namespace}
  defp normalize_timezone_source("application_default"), do: {:ok, :application_default}
  defp normalize_timezone_source("utc_fallback"), do: {:ok, :utc_fallback}
  defp normalize_timezone_source(source), do: {:error, {:invalid_window_timezone_source, source}}
end

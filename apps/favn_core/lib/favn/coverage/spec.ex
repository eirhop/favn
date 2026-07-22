defmodule Favn.Coverage.Spec do
  @moduledoc """
  Authored historical coverage policy for one windowed asset.

  Coverage declares the first required window and how the last expected window
  is selected. It does not schedule work or delay execution. Manifest
  generation resolves this portable declaration against the asset window and
  deployment environment into `Favn.Coverage.Effective`.
  """

  alias Favn.Window.Validate

  @typedoc "A fixed inclusive boundary or a moving expected-window policy."
  @type through :: :latest_closed | :current | Date.t() | DateTime.t()

  @type t :: %__MODULE__{
          from: Date.t() | DateTime.t(),
          through: through(),
          availability_delay_seconds: non_neg_integer()
        }

  @enforce_keys [:from]
  defstruct [:from, through: :latest_closed, availability_delay_seconds: 0]

  @units %{
    second: 1,
    seconds: 1,
    minute: 60,
    minutes: 60,
    hour: 3_600,
    hours: 3_600,
    day: 86_400,
    days: 86_400
  }

  @persisted_fields [
    :from,
    :through,
    :availability_delay_seconds,
    "from",
    "through",
    "availability_delay_seconds"
  ]

  @doc """
  Builds and validates an authored coverage policy.

  `availability_delay` uses `{unit, amount}` and is accepted only with
  `through: :latest_closed`.
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_keyword(opts),
         :ok <- validate_duplicates(opts),
         :ok <- Validate.strict_keyword_opts(opts, [:from, :through, :availability_delay]),
         {:ok, from} <- fetch_from(opts),
         {:ok, through} <- validate_through(Keyword.get(opts, :through, :latest_closed)),
         {:ok, delay} <- normalize_delay(Keyword.get(opts, :availability_delay, {:seconds, 0})),
         :ok <- validate_delay_policy(opts, through),
         :ok <- validate_fixed_order(from, through) do
      {:ok,
       %__MODULE__{
         from: from,
         through: through,
         availability_delay_seconds: delay
       }}
    end
  end

  def new(value), do: {:error, {:invalid_coverage_options, value}}

  @doc "Builds a coverage policy and raises on invalid input."
  @spec new!(keyword()) :: t()
  def new!(opts) do
    case new(opts) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid coverage policy: #{inspect(reason)}"
    end
  end

  @doc "Validates an already normalized coverage policy."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = spec) do
    with {:ok, from} <- validate_boundary(:from, spec.from),
         {:ok, through} <- validate_through(spec.through),
         :ok <- validate_delay_seconds(spec.availability_delay_seconds),
         :ok <- validate_normalized_delay_policy(spec),
         :ok <- validate_fixed_order(from, through) do
      {:ok, %{spec | from: from, through: through}}
    end
  end

  @doc "Normalizes a persisted or authored coverage value."
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = spec), do: validate(spec)

  def from_value(value) when is_list(value) do
    if Keyword.keyword?(value), do: new(value), else: {:error, {:invalid_coverage_options, value}}
  end

  def from_value(value) when is_map(value) do
    with :ok <- reject_unknown_fields(value),
         {:ok, from} <- decode_boundary(field_value(value, :from)),
         {:ok, through} <- decode_through(field_value(value, :through, :latest_closed)),
         delay when is_integer(delay) <- field_value(value, :availability_delay_seconds, 0) do
      validate(%__MODULE__{
        from: from,
        through: through,
        availability_delay_seconds: delay
      })
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_coverage_policy, value}}
    end
  end

  def from_value(value), do: {:error, {:invalid_coverage_policy, value}}

  defp validate_keyword(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, {:invalid_coverage_options, opts}}
  end

  defp validate_duplicates(opts) do
    duplicates =
      opts
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(fn {_key, count} -> count > 1 end)
      |> Enum.map(&elem(&1, 0))

    if duplicates == [], do: :ok, else: {:error, {:duplicate_coverage_options, duplicates}}
  end

  defp fetch_from(opts) do
    case Keyword.fetch(opts, :from) do
      {:ok, value} -> validate_boundary(:from, value)
      :error -> {:error, :coverage_from_required}
    end
  end

  defp validate_boundary(_field, %Date{} = value), do: {:ok, value}
  defp validate_boundary(_field, %DateTime{} = value), do: {:ok, value}
  defp validate_boundary(field, value), do: {:error, {:invalid_coverage_boundary, field, value}}

  defp validate_through(value) when value in [:latest_closed, :current], do: {:ok, value}
  defp validate_through(value), do: validate_boundary(:through, value)

  defp normalize_delay({unit, amount}) when is_integer(amount) and amount >= 0 do
    case Map.fetch(@units, unit) do
      {:ok, multiplier} -> {:ok, multiplier * amount}
      :error -> {:error, {:invalid_coverage_delay_unit, unit}}
    end
  end

  defp normalize_delay(value), do: {:error, {:invalid_coverage_delay, value}}

  defp validate_delay_policy(opts, :latest_closed), do: validate_delay_option_shape(opts)

  defp validate_delay_policy(opts, through) do
    if Keyword.has_key?(opts, :availability_delay) do
      {:error, {:coverage_delay_requires_latest_closed, through}}
    else
      :ok
    end
  end

  defp validate_delay_option_shape(_opts), do: :ok

  defp validate_delay_seconds(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_delay_seconds(value), do: {:error, {:invalid_coverage_delay_seconds, value}}

  defp validate_normalized_delay_policy(%__MODULE__{
         through: through,
         availability_delay_seconds: delay
       })
       when through != :latest_closed and delay != 0,
       do: {:error, {:coverage_delay_requires_latest_closed, through}}

  defp validate_normalized_delay_policy(_spec), do: :ok

  defp validate_fixed_order(%Date{} = from, %Date{} = through) do
    if Date.compare(through, from) == :lt,
      do: {:error, {:coverage_through_before_from, from, through}},
      else: :ok
  end

  defp validate_fixed_order(%DateTime{} = from, %DateTime{} = through) do
    if DateTime.compare(through, from) == :lt,
      do: {:error, {:coverage_through_before_from, from, through}},
      else: :ok
  end

  defp validate_fixed_order(_from, _through), do: :ok

  defp decode_boundary(%Date{} = value), do: {:ok, value}
  defp decode_boundary(%DateTime{} = value), do: {:ok, value}

  defp decode_boundary(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _reason} -> DateTime.from_iso8601(value) |> normalize_datetime_decode()
    end
  end

  defp decode_boundary(value), do: {:error, {:invalid_coverage_boundary, value}}

  defp decode_through(value) when value in [:latest_closed, :current], do: {:ok, value}
  defp decode_through("latest_closed"), do: {:ok, :latest_closed}
  defp decode_through("current"), do: {:ok, :current}
  defp decode_through(value), do: decode_boundary(value)

  defp normalize_datetime_decode({:ok, datetime, _offset}), do: {:ok, datetime}
  defp normalize_datetime_decode(_error), do: {:error, :invalid_coverage_datetime}

  defp field_value(map, key, default \\ nil) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp reject_unknown_fields(value) do
    unknown =
      value
      |> Map.keys()
      |> Enum.reject(&(&1 in @persisted_fields))
      |> Enum.sort_by(&inspect/1)

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_coverage_spec_fields, unknown}}
  end
end

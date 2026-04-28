defmodule Favn.Window.Spec do
  @moduledoc """
  Canonical asset-level runtime window specification.

  A window spec describes how an asset is windowed independently from any
  particular run request.
  """

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

defmodule Favn.Window.Anchor do
  @moduledoc """
  Run-level anchor window request.

  An anchor window represents execution intent from scheduler/operator/pipeline.
  """

  alias Favn.Window.Key

  @type kind :: :hour | :day | :month

  @type t :: %__MODULE__{
          kind: kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t(),
          key: Key.t()
        }

  defstruct [:kind, :start_at, :end_at, :key, timezone: "Etc/UTC"]

  @spec new(kind(), DateTime.t(), DateTime.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) when is_list(opts) do
    timezone = Keyword.get(opts, :timezone, "Etc/UTC")

    with :ok <- validate_kind(kind),
         :ok <- validate_order(start_at, end_at),
         :ok <- validate_timezone(timezone),
         {:ok, key} <- Key.new(kind, start_at, timezone) do
      {:ok,
       %__MODULE__{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone, key: key}}
    end
  end

  @spec new!(kind(), DateTime.t(), DateTime.t(), keyword()) :: t()
  def new!(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) do
    case new(kind, start_at, end_at, opts) do
      {:ok, anchor} -> anchor
      {:error, reason} -> raise ArgumentError, "invalid anchor window: #{inspect(reason)}"
    end
  end

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = anchor) do
    with :ok <- validate_kind(anchor.kind),
         :ok <- validate_order(anchor.start_at, anchor.end_at),
         :ok <- validate_timezone(anchor.timezone),
         :ok <- validate_key(anchor) do
      :ok
    end
  end

  defp validate_key(%__MODULE__{} = anchor) do
    expected = Key.new!(anchor.kind, anchor.start_at, anchor.timezone)
    if anchor.key == expected, do: :ok, else: {:error, :invalid_key}
  end

  defp validate_kind(kind) when kind in [:hour, :day, :month], do: :ok
  defp validate_kind(kind), do: {:error, {:invalid_kind, kind}}

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _ -> {:error, :invalid_window_bounds}
    end
  end

  defp validate_timezone(timezone) when is_binary(timezone) and byte_size(timezone) > 0, do: :ok
  defp validate_timezone(timezone), do: {:error, {:invalid_timezone, timezone}}
end

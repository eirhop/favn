defmodule Favn.Window.Runtime do
  @moduledoc """
  Concrete runtime window used by one asset execution node.
  """

  alias Favn.Window.Key
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month

  @type t :: %__MODULE__{
          kind: kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t(),
          key: Key.t(),
          anchor_key: Key.t()
        }

  defstruct [:kind, :start_at, :end_at, :key, :anchor_key, timezone: "Etc/UTC"]

  @spec new(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def new(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ [])
      when is_map(anchor_key) and is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         :ok <- Validate.kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- validate_order(start_at, end_at),
         :ok <- Validate.timezone(timezone),
         :ok <- validate_key(anchor_key),
         {:ok, key} <- Key.new(kind, start_at, timezone) do
      {:ok,
       %__MODULE__{
         kind: kind,
         start_at: start_at,
         end_at: end_at,
         timezone: timezone,
         key: key,
         anchor_key: anchor_key
       }}
    end
  end

  @spec new!(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) :: t()
  def new!(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    case new(kind, start_at, end_at, anchor_key, opts) do
      {:ok, runtime} -> runtime
      {:error, reason} -> raise ArgumentError, "invalid runtime window: #{inspect(reason)}"
    end
  end

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = runtime) do
    with :ok <- Validate.kind(runtime.kind),
         :ok <- validate_order(runtime.start_at, runtime.end_at),
         :ok <- Validate.timezone(runtime.timezone),
         :ok <- validate_key(runtime.key),
         :ok <- validate_key(runtime.anchor_key) do
      expected = Key.new!(runtime.kind, runtime.start_at, runtime.timezone)
      if runtime.key == expected, do: :ok, else: {:error, :invalid_key}
    end
  end

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _ -> {:error, :invalid_window_bounds}
    end
  end

  defp validate_key(%{kind: kind, start_at_us: start_at_us, timezone: timezone})
       when kind in [:hour, :day, :month] and is_integer(start_at_us) and is_binary(timezone),
       do: :ok

  defp validate_key(_value), do: {:error, :invalid_key}
end

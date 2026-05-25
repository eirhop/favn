defmodule Favn.SQL.Deadline do
  @moduledoc """
  Finite deadline contract for SQL runtime operations.

  `timeout_ms` is the caller-visible budget. `started_at` and `deadline_at`
  use monotonic milliseconds so remaining time can be recomputed safely while an
  operation moves through admission, pooling, and adapter execution.
  """

  @enforce_keys [:timeout_ms, :started_at, :deadline_at]
  defstruct [:timeout_ms, :started_at, :deadline_at]

  @type t :: %__MODULE__{
          timeout_ms: pos_integer(),
          started_at: integer(),
          deadline_at: integer()
        }

  @doc """
  Builds a deadline from a finite timeout in milliseconds.
  """
  @spec new(pos_integer()) :: t()
  def new(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    started_at = monotonic_ms()

    %__MODULE__{
      timeout_ms: timeout_ms,
      started_at: started_at,
      deadline_at: started_at + timeout_ms
    }
  end

  @doc """
  Returns the deadline in `opts`, or creates one from `:timeout_ms`/`default_timeout_ms`.
  """
  @spec from_opts(keyword(), pos_integer()) :: t()
  def from_opts(opts, default_timeout_ms) when is_list(opts) do
    case Keyword.get(opts, :deadline) do
      %__MODULE__{} = deadline -> deadline
      _other -> new(timeout_from_opts(opts, default_timeout_ms))
    end
  end

  @doc """
  Returns the remaining whole milliseconds before the deadline expires.
  """
  @spec remaining_ms(t()) :: non_neg_integer()
  def remaining_ms(%__MODULE__{deadline_at: deadline_at}) do
    max(deadline_at - monotonic_ms(), 0)
  end

  @doc """
  Returns true when no time remains.
  """
  @spec expired?(t()) :: boolean()
  def expired?(%__MODULE__{} = deadline), do: remaining_ms(deadline) == 0

  defp timeout_from_opts(opts, default_timeout_ms) do
    case Keyword.get(opts, :timeout_ms, default_timeout_ms) do
      value when is_integer(value) and value > 0 -> value
      _other -> default_timeout_ms
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

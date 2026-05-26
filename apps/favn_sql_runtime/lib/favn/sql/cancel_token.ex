defmodule Favn.SQL.CancelToken do
  @moduledoc """
  SQL-runtime cancellation token passed to adapter operations.

  The token is intentionally SQL-owned: callers may carry an opaque operation id
  and optional deadline, while adapters decide whether they can use it for native
  cancellation. Adapters that do not support native cancellation can ignore it and
  report `:native_cancel_unsupported` through their own diagnostics/outcomes.
  """

  @enforce_keys [:operation_id, :created_at]
  defstruct [:operation_id, :deadline_at, :created_at]

  @type t :: %__MODULE__{
          operation_id: String.t(),
          deadline_at: DateTime.t() | nil,
          created_at: DateTime.t()
        }

  @doc "Builds a cancellation token for one SQL operation scope."
  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    %__MODULE__{
      operation_id: Keyword.get(opts, :operation_id, new_operation_id()),
      deadline_at: Keyword.get(opts, :deadline_at),
      created_at: DateTime.utc_now()
    }
  end

  defp new_operation_id do
    "sqlop_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end

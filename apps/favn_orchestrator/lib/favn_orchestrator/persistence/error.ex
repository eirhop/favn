defmodule FavnOrchestrator.Persistence.Error do
  @moduledoc """
  Stable persistence failure returned across the orchestrator boundary.

  Database driver errors stay inside the PostgreSQL implementation. Callers use
  `kind` to decide whether an operation may be retried and may log `message` and
  `details`, which must already be redacted.
  """

  @type kind ::
          :conflict
          | :fenced
          | :not_found
          | :invalid
          | :forbidden
          | :limit_exceeded
          | :timeout
          | :unavailable
          | :constraint
          | :unsupported
          | :internal

  @enforce_keys [:kind, :message]
  defstruct [:kind, :message, retryable?: false, details: %{}]

  @type t :: %__MODULE__{
          kind: kind(),
          message: String.t(),
          retryable?: boolean(),
          details: map()
        }

  @doc "Creates a redacted persistence error."
  @spec new(kind(), String.t(), keyword()) :: t()
  def new(kind, message, opts \\ []) when is_atom(kind) and is_binary(message) do
    %__MODULE__{
      kind: kind,
      message: message,
      retryable?: Keyword.get(opts, :retryable?, false),
      details: Keyword.get(opts, :details, %{})
    }
  end
end

defmodule FavnOrchestrator.Persistence.Identity do
  @moduledoc """
  Shared byte-length contract for identities crossing persistence boundaries.

  Persistence adapters may enforce the same limit at the storage layer, but
  orchestrator callers validate first so failures retain field-level context.
  """

  alias FavnOrchestrator.Persistence.Error

  @max_bytes 255

  @doc "Returns the supported maximum byte length for persisted identities."
  @spec max_bytes() :: pos_integer()
  def max_bytes, do: @max_bytes

  @doc "Returns true when a value is a non-empty persistence identity."
  @spec valid?(term()) :: boolean()
  def valid?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= @max_bytes

  @doc "Validates one named persistence identity with operator-facing details."
  @spec validate(atom(), term()) :: :ok | {:error, Error.t()}
  def validate(_field, value)
      when is_binary(value) and value != "" and byte_size(value) <= @max_bytes,
      do: :ok

  def validate(field, value) when is_atom(field) and is_binary(value) do
    actual_bytes = byte_size(value)

    {:error,
     Error.new(
       :invalid,
       "persistence identity #{field} is #{actual_bytes} bytes; supported maximum is #{@max_bytes} bytes",
       details: %{field: field, actual_bytes: actual_bytes, max_bytes: @max_bytes}
     )}
  end

  def validate(field, _value) when is_atom(field) do
    {:error,
     Error.new(:invalid, "persistence identity #{field} must be a non-empty string",
       details: %{field: field, max_bytes: @max_bytes}
     )}
  end

  @doc "Validates named identities in order and returns the first failure."
  @spec validate_many([{atom(), term()}]) :: :ok | {:error, Error.t()}
  def validate_many(identities) when is_list(identities) do
    Enum.reduce_while(identities, :ok, fn {field, value}, :ok ->
      case validate(field, value) do
        :ok -> {:cont, :ok}
        {:error, %Error{}} = error -> {:halt, error}
      end
    end)
  end
end

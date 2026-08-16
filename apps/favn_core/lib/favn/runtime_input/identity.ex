defmodule Favn.RuntimeInput.Identity do
  @moduledoc """
  Shared validation contract for a runtime-input identity.

  Identities may describe external resources, but validation errors never retain
  or expose their values.
  """

  @max_bytes 1_024

  @type reason :: :not_a_string | :empty | :blank | :invalid_utf8 | :too_large
  @type error ::
          {:invalid_runtime_input_identity,
           %{
             required(:field) => :input_identity,
             required(:limit_bytes) => pos_integer(),
             required(:reason) => reason()
           }}

  @doc "Returns the maximum encoded size of a runtime-input identity."
  @spec max_bytes() :: pos_integer()
  def max_bytes, do: @max_bytes

  @doc "Returns a redacted description of the runtime-input identity contract."
  @spec error_message() :: String.t()
  def error_message do
    "input_identity must be a non-empty UTF-8 string of at most #{@max_bytes} bytes"
  end

  @doc "Validates a runtime-input identity without retaining its value on failure."
  @spec validate(term()) :: :ok | {:error, error()}
  def validate(value) do
    cond do
      not is_binary(value) -> error(:not_a_string)
      value == "" -> error(:empty)
      byte_size(value) > @max_bytes -> error(:too_large)
      not String.valid?(value) -> error(:invalid_utf8)
      String.trim(value) == "" -> error(:blank)
      true -> :ok
    end
  end

  @doc """
  Validates an identity read from an existing pin.

  Historical pin codecs accepted whitespace-only identities. Decoding keeps
  those pins readable while applying the shared byte limit to every stored
  identity. New resolutions and writes must use `validate/1`.
  """
  @spec validate_stored(term()) :: :ok | {:error, error()}
  def validate_stored(value) do
    cond do
      not is_binary(value) -> error(:not_a_string)
      value == "" -> error(:empty)
      byte_size(value) > @max_bytes -> error(:too_large)
      not String.valid?(value) -> error(:invalid_utf8)
      true -> :ok
    end
  end

  defp error(reason) do
    {:error,
     {:invalid_runtime_input_identity,
      %{field: :input_identity, limit_bytes: @max_bytes, reason: reason}}}
  end
end

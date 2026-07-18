defmodule FavnStoragePostgres.Payload do
  @moduledoc false

  alias FavnStoragePostgres.CanonicalJSON

  @spec validate(term(), pos_integer()) :: :ok | {:error, :invalid_or_oversized_payload}
  def validate(value, maximum_bytes) when is_integer(maximum_bytes) and maximum_bytes > 0 do
    case CanonicalJSON.encode(value) do
      {:ok, encoded} when byte_size(encoded) <= maximum_bytes -> :ok
      _invalid_or_oversized -> {:error, :invalid_or_oversized_payload}
    end
  end

  @spec validate_encoded(binary(), pos_integer()) ::
          :ok | {:error, :invalid_or_oversized_payload}
  def validate_encoded(encoded, maximum_bytes)
      when is_binary(encoded) and is_integer(maximum_bytes) and maximum_bytes > 0 do
    if byte_size(encoded) <= maximum_bytes,
      do: :ok,
      else: {:error, :invalid_or_oversized_payload}
  end
end

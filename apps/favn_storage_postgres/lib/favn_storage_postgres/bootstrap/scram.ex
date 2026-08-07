defmodule FavnStoragePostgres.Bootstrap.Scram do
  @moduledoc false

  @key_length 32
  @salt_length 16

  @doc false
  @spec password_supported?(String.t()) :: boolean()
  def password_supported?(password) when is_binary(password) do
    password != "" and
      Enum.all?(:binary.bin_to_list(password), &(&1 in 0x21..0x7E))
  end

  @doc false
  @spec verifier(String.t(), pos_integer(), binary()) :: String.t()
  def verifier(password, iterations, salt \\ :crypto.strong_rand_bytes(@salt_length))
      when is_binary(password) and is_integer(iterations) and iterations > 0 and
             is_binary(salt) and byte_size(salt) >= 16 do
    salted_password =
      :crypto.pbkdf2_hmac(:sha256, password, salt, iterations, @key_length)

    client_key = :crypto.mac(:hmac, :sha256, salted_password, "Client Key")
    stored_key = :crypto.hash(:sha256, client_key)
    server_key = :crypto.mac(:hmac, :sha256, salted_password, "Server Key")

    "SCRAM-SHA-256$#{iterations}:#{Base.encode64(salt)}$#{Base.encode64(stored_key)}:#{Base.encode64(server_key)}"
  end
end

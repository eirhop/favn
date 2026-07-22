defmodule FavnOrchestrator.Storage.RuntimeInputPinCodec do
  @moduledoc false

  alias Favn.RuntimeInput.Pin

  @aad "favn.runtime_input_pin.v1"

  @spec node_key_hash(Favn.Plan.node_key()) :: String.t()
  def node_key_hash(node_key) do
    node_key
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec encode(Pin.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def encode(%Pin{} = pin, opts) when is_list(opts) do
    payload = :erlang.term_to_binary(pin, [:deterministic])

    if pin.sensitive_params == [] do
      {:ok, "plain.v1." <> Base.url_encode64(payload, padding: false)}
    else
      with {:ok, key} <- encryption_key(opts) do
        nonce = :crypto.strong_rand_bytes(12)

        {ciphertext, tag} =
          :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, payload, @aad, true)

        {:ok,
         Enum.join(
           ["aesgcm.v1", encode64(nonce), encode64(tag), encode64(ciphertext)],
           "."
         )}
      end
    end
  end

  @spec decode(String.t(), keyword()) :: {:ok, Pin.t()} | {:error, term()}
  def decode("plain.v1." <> encoded, _opts), do: decode_term(encoded)

  def decode("aesgcm.v1." <> encoded, opts) do
    with [nonce64, tag64, ciphertext64] <- String.split(encoded, "."),
         {:ok, nonce} <- decode64(nonce64),
         {:ok, tag} <- decode64(tag64),
         {:ok, ciphertext} <- decode64(ciphertext64),
         {:ok, key} <- encryption_key(opts),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(
             :aes_256_gcm,
             key,
             nonce,
             ciphertext,
             @aad,
             tag,
             false
           ) do
      safe_binary_to_pin(plaintext)
    else
      :error -> {:error, :runtime_input_pin_decryption_failed}
      {:error, _reason} = error -> error
      _other -> {:error, :runtime_input_pin_decryption_failed}
    end
  end

  def decode(_payload, _opts), do: {:error, :invalid_runtime_input_pin_payload}

  defp decode_term(encoded) do
    with {:ok, binary} <- decode64(encoded), do: safe_binary_to_pin(binary)
  end

  # sobelow_skip ["Misc.BinToTerm"]
  defp safe_binary_to_pin(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      %Pin{} = pin -> {:ok, pin}
      _other -> {:error, :invalid_runtime_input_pin_payload}
    end
  rescue
    _error -> {:error, :invalid_runtime_input_pin_payload}
  end

  defp encryption_key(opts) do
    case Keyword.get(opts, :runtime_input_pin_key) do
      key when is_binary(key) and byte_size(key) == 32 -> {:ok, key}
      encoded when is_binary(encoded) -> decode_key(encoded)
      _missing -> {:error, :runtime_input_pin_encryption_key_required}
    end
  end

  defp decode_key(encoded) do
    with {:ok, key} <- Base.decode64(encoded),
         32 <- byte_size(key) do
      {:ok, key}
    else
      _other -> {:error, :invalid_runtime_input_pin_encryption_key}
    end
  end

  defp encode64(value), do: Base.url_encode64(value, padding: false)

  defp decode64(value) do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> {:error, :invalid_runtime_input_pin_payload}
    end
  end
end

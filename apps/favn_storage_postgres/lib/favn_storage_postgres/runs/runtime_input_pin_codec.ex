defmodule FavnStoragePostgres.Runs.RuntimeInputPinCodec do
  @moduledoc false

  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Storage.PayloadCodec

  @format "favn.runtime_input_pin.storage.v2"
  @nonce_bytes 12
  @tag_bytes 16
  @max_plaintext_bytes 262_144

  @type scope :: %{
          required(:workspace_id) => String.t(),
          required(:run_id) => String.t(),
          required(:node_key_hash) => binary(),
          required(:key_version) => pos_integer()
        }

  @spec node_key_hash(Favn.Plan.node_key()) :: {:ok, binary()} | {:error, term()}
  def node_key_hash(node_key) do
    with {:ok, encoded} <- PayloadCodec.encode(node_key) do
      {:ok, :crypto.hash(:sha256, encoded)}
    end
  end

  @spec encode(Pin.t(), scope(), binary()) ::
          {:ok, %{payload: binary(), payload_fingerprint: binary()}} | {:error, term()}
  def encode(%Pin{} = pin, scope, key) when byte_size(key) == 32 do
    with :ok <- validate_scope(pin, scope),
         {:ok, plaintext} <- encode_plaintext(pin),
         :ok <- validate_size(plaintext),
         {:ok, aad} <- aad(scope) do
      nonce = :crypto.strong_rand_bytes(@nonce_bytes)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, plaintext, aad, true)

      {:ok,
       %{
         payload: nonce <> tag <> ciphertext,
         payload_fingerprint: :crypto.mac(:hmac, :sha256, key, plaintext)
       }}
    end
  end

  def encode(%Pin{}, _scope, _key), do: {:error, :invalid_runtime_input_pin_key}

  @spec decode(binary(), scope(), binary(), MapSet.t(String.t())) ::
          {:ok, Pin.t()} | {:error, term()}
  def decode(payload, scope, key, allowed_resolvers)
      when is_binary(payload) and byte_size(key) == 32 do
    with true <- byte_size(payload) > @nonce_bytes + @tag_bytes,
         <<nonce::binary-size(@nonce_bytes), tag::binary-size(@tag_bytes), ciphertext::binary>> <-
           payload,
         {:ok, aad} <- aad(scope),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(
             :aes_256_gcm,
             key,
             nonce,
             ciphertext,
             aad,
             tag,
             false
           ),
         :ok <- validate_size(plaintext),
         {:ok, pin} <- decode_plaintext(plaintext, allowed_resolvers),
         :ok <- validate_scope(pin, scope) do
      {:ok, pin}
    else
      false -> {:error, :runtime_input_pin_decryption_failed}
      :error -> {:error, :runtime_input_pin_decryption_failed}
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :runtime_input_pin_decryption_failed}
    end
  rescue
    _error -> {:error, :runtime_input_pin_decryption_failed}
  end

  def decode(_payload, _scope, _key, _allowed_resolvers),
    do: {:error, :invalid_runtime_input_pin_key}

  defp encode_plaintext(pin) do
    with {:ok, node_key} <- PayloadCodec.encode(pin.node_key),
         {:ok, params} <- PayloadCodec.encode(pin.params),
         {:ok, metadata} <- PayloadCodec.encode(pin.metadata),
         {:ok, sensitive_params} <- PayloadCodec.encode(pin.sensitive_params),
         {:ok, source_node_key} <- encode_optional(pin.source_node_key) do
      Jason.encode(%{
        "format" => @format,
        "schema_version" => pin.schema_version,
        "run_id" => pin.run_id,
        "node_key" => node_key,
        "resolver" => Atom.to_string(pin.resolver),
        "params" => params,
        "input_identity" => pin.input_identity,
        "metadata" => metadata,
        "sensitive_params" => sensitive_params,
        "payload_fingerprint" => pin.payload_fingerprint,
        "source_run_id" => pin.source_run_id,
        "source_node_key" => source_node_key,
        "source_payload_fingerprint" => pin.source_payload_fingerprint,
        "inserted_at" => DateTime.to_iso8601(pin.inserted_at),
        "updated_at" => DateTime.to_iso8601(pin.updated_at)
      })
    end
  end

  defp decode_plaintext(plaintext, allowed_resolvers) do
    with {:ok, dto} <- Jason.decode(plaintext),
         %{"format" => @format, "schema_version" => 1} <- dto,
         {:ok, node_key} <- PayloadCodec.decode(dto["node_key"]),
         {:ok, resolver} <- decode_resolver(dto["resolver"], allowed_resolvers),
         {:ok, params} <- PayloadCodec.decode(dto["params"]),
         {:ok, metadata} <- PayloadCodec.decode(dto["metadata"]),
         {:ok, sensitive_params} <- PayloadCodec.decode(dto["sensitive_params"]),
         {:ok, source_node_key} <- decode_optional(dto["source_node_key"]),
         {:ok, inserted_at} <- decode_datetime(dto["inserted_at"]),
         {:ok, updated_at} <- decode_datetime(dto["updated_at"]),
         true <- valid_decoded_fields?(dto, node_key, params, metadata, sensitive_params) do
      {:ok,
       %Pin{
         run_id: dto["run_id"],
         node_key: node_key,
         resolver: resolver,
         params: params,
         input_identity: dto["input_identity"],
         metadata: metadata,
         sensitive_params: sensitive_params,
         payload_fingerprint: dto["payload_fingerprint"],
         source_run_id: dto["source_run_id"],
         source_node_key: source_node_key,
         source_payload_fingerprint: dto["source_payload_fingerprint"],
         schema_version: 1,
         inserted_at: inserted_at,
         updated_at: updated_at
       }}
    else
      false -> {:error, :invalid_runtime_input_pin_payload}
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :invalid_runtime_input_pin_payload}
    end
  end

  defp encode_optional(nil), do: {:ok, nil}
  defp encode_optional(value), do: PayloadCodec.encode(value)
  defp decode_optional(nil), do: {:ok, nil}
  defp decode_optional(value) when is_binary(value), do: PayloadCodec.decode(value)
  defp decode_optional(_value), do: {:error, :invalid_runtime_input_pin_payload}

  defp decode_resolver(resolver, allowed_resolvers) when is_binary(resolver) do
    if MapSet.member?(allowed_resolvers, resolver) do
      {:ok, String.to_existing_atom(resolver)}
    else
      {:error, :runtime_input_pin_resolver_not_in_manifest}
    end
  rescue
    ArgumentError -> {:error, :runtime_input_pin_resolver_not_loaded}
  end

  defp decode_resolver(_resolver, _allowed_resolvers),
    do: {:error, :invalid_runtime_input_pin_payload}

  defp decode_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> {:ok, datetime}
      _invalid -> {:error, :invalid_runtime_input_pin_payload}
    end
  end

  defp decode_datetime(_value), do: {:error, :invalid_runtime_input_pin_payload}

  defp valid_decoded_fields?(dto, node_key, params, metadata, sensitive_params) do
    valid_identity?(dto["run_id"]) and is_tuple(node_key) and is_map(params) and is_map(metadata) and
      is_list(sensitive_params) and Enum.all?(sensitive_params, &(is_atom(&1) or is_binary(&1))) and
      valid_identity?(dto["input_identity"]) and valid_identity?(dto["payload_fingerprint"]) and
      optional_identity?(dto["source_run_id"]) and
      optional_identity?(dto["source_payload_fingerprint"])
  end

  defp validate_scope(%Pin{} = pin, scope) do
    with true <- pin.run_id == scope.run_id,
         {:ok, node_key_hash} <- node_key_hash(pin.node_key),
         true <- :crypto.hash_equals(node_key_hash, scope.node_key_hash) do
      :ok
    else
      _invalid -> {:error, :runtime_input_pin_scope_mismatch}
    end
  end

  defp aad(scope) do
    Jason.encode([
      @format,
      scope.workspace_id,
      scope.run_id,
      Base.encode16(scope.node_key_hash, case: :lower),
      scope.key_version
    ])
  end

  defp validate_size(payload) when byte_size(payload) <= @max_plaintext_bytes, do: :ok
  defp validate_size(_payload), do: {:error, :runtime_input_pin_payload_too_large}

  defp valid_identity?(value), do: is_binary(value) and value != "" and byte_size(value) <= 512
  defp optional_identity?(nil), do: true
  defp optional_identity?(value), do: valid_identity?(value)
end

defmodule FavnStoragePostgres.StorageV2.RuntimeInputPinCodecTest do
  use ExUnit.Case, async: true

  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnStoragePostgres.Runs.RuntimeInputPinCodec

  @format "favn.runtime_input_pin.storage.v2"
  @key "0123456789abcdef0123456789abcdef"

  test "decodes legacy whitespace identities but rejects them on new writes" do
    pin = legacy_pin("   ")
    scope = scope(pin)
    payload = legacy_payload(pin, scope)

    assert {:ok, decoded} =
             RuntimeInputPinCodec.decode(
               payload,
               scope,
               @key,
               MapSet.new([Atom.to_string(__MODULE__)])
             )

    assert decoded == pin

    assert {:error,
            {:invalid_runtime_input_identity,
             %{field: :input_identity, limit_bytes: 1_024, reason: :blank}}} =
             RuntimeInputPinCodec.encode(pin, scope, @key)
  end

  test "applies the shared byte limit while decoding legacy payloads" do
    pin = legacy_pin(String.duplicate("i", 1_025))
    scope = scope(pin)

    assert {:error,
            {:invalid_runtime_input_identity,
             %{field: :input_identity, limit_bytes: 1_024, reason: :too_large}}} =
             pin
             |> legacy_payload(scope)
             |> RuntimeInputPinCodec.decode(
               scope,
               @key,
               MapSet.new([Atom.to_string(__MODULE__)])
             )
  end

  defp legacy_pin(input_identity) do
    {:ok, resolution} =
      Resolution.new(
        resolver: __MODULE__,
        params: %{account_id: 42},
        input_identity: "valid-before-legacy-fixture"
      )

    "run-runtime-input-codec"
    |> Pin.new({{__MODULE__, :asset}, nil}, resolution)
    |> Map.put(:input_identity, input_identity)
  end

  defp scope(pin) do
    {:ok, node_key_hash} = RuntimeInputPinCodec.node_key_hash(pin.node_key)

    %{
      workspace_id: "workspace-runtime-input-codec",
      run_id: pin.run_id,
      node_key_hash: node_key_hash,
      key_version: 1
    }
  end

  defp legacy_payload(pin, scope) do
    plaintext =
      Jason.encode!(%{
        "format" => @format,
        "schema_version" => pin.schema_version,
        "run_id" => pin.run_id,
        "node_key" => encode(pin.node_key),
        "resolver" => Atom.to_string(pin.resolver),
        "params" => encode(pin.params),
        "input_identity" => pin.input_identity,
        "metadata" => encode(pin.metadata),
        "sensitive_params" => encode(pin.sensitive_params),
        "payload_fingerprint" => pin.payload_fingerprint,
        "source_run_id" => pin.source_run_id,
        "source_node_key" => nil,
        "source_payload_fingerprint" => pin.source_payload_fingerprint,
        "inserted_at" => DateTime.to_iso8601(pin.inserted_at),
        "updated_at" => DateTime.to_iso8601(pin.updated_at)
      })

    aad =
      Jason.encode!([
        @format,
        scope.workspace_id,
        scope.run_id,
        Base.encode16(scope.node_key_hash, case: :lower),
        scope.key_version
      ])

    nonce = :binary.copy(<<0>>, 12)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, @key, nonce, plaintext, aad, true)

    nonce <> tag <> ciphertext
  end

  defp encode(value) do
    {:ok, encoded} = PayloadCodec.encode(value)
    encoded
  end
end

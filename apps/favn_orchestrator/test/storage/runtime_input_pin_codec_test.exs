defmodule FavnOrchestrator.Storage.RuntimeInputPinCodecTest do
  use ExUnit.Case, async: true

  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Storage.RuntimeInputPinCodec

  @node_key {{MyApp.RuntimeInputAsset, :asset}, nil}
  @key :crypto.hash(:sha256, "runtime-input-pin-test-key")

  test "sensitive pins require encryption and fail closed with the wrong key" do
    pin = pin([:token])

    assert {:error, :runtime_input_pin_encryption_key_required} =
             RuntimeInputPinCodec.encode(pin, [])

    assert {:ok, "aesgcm.v1." <> _rest = payload} =
             RuntimeInputPinCodec.encode(pin, runtime_input_pin_key: @key)

    refute payload =~ "secret-token"
    assert {:ok, ^pin} = RuntimeInputPinCodec.decode(payload, runtime_input_pin_key: @key)

    assert {:error, :runtime_input_pin_decryption_failed} =
             RuntimeInputPinCodec.decode(payload,
               runtime_input_pin_key: :crypto.hash(:sha256, "wrong-key")
             )
  end

  test "non-sensitive pins use the versioned plain codec" do
    pin = pin([])

    assert {:ok, "plain.v1." <> _rest = payload} = RuntimeInputPinCodec.encode(pin, [])
    assert {:ok, ^pin} = RuntimeInputPinCodec.decode(payload, [])
  end

  defp pin(sensitive_params) do
    {:ok, resolution} =
      Resolution.new(%{
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: 42, token: "secret-token"},
        input_identity: "test-input",
        metadata: %{source: "test"},
        sensitive_params: sensitive_params
      })

    Pin.new("pin-codec-run", @node_key, resolution)
  end
end

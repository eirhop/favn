defmodule Favn.RuntimeInput.IdentityTest do
  use ExUnit.Case, async: true

  alias Favn.RuntimeInput.Identity
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution

  test "resolution and pin validation share the 1,024-byte identity boundary" do
    for size <- [255, 256, 512, 513, 1_024] do
      identity = identity(size)

      assert {:ok, resolution} =
               Resolution.new(
                 resolver: __MODULE__,
                 params: %{},
                 input_identity: identity
               )

      pin = Pin.new("run-identity-boundary", {{__MODULE__, :asset}, nil}, resolution)

      assert byte_size(resolution.input_identity) == size
      assert :ok = Pin.validate_input_identity(pin)
    end
  end

  test "the earliest public boundary rejects an oversized identity without retaining it" do
    oversized = identity(1_025)

    assert {:error,
            {:invalid_runtime_input_identity,
             %{field: :input_identity, limit_bytes: 1_024, reason: :too_large}} = error} =
             Resolution.new(
               resolver: __MODULE__,
               params: %{},
               input_identity: oversized
             )

    refute inspect(error) =~ oversized
  end

  test "identity validation requires a non-blank UTF-8 string" do
    for {value, reason} <- [
          {nil, :not_a_string},
          {"", :empty},
          {"   ", :blank},
          {<<255>>, :invalid_utf8}
        ] do
      assert {:error,
              {:invalid_runtime_input_identity,
               %{field: :input_identity, limit_bytes: 1_024, reason: ^reason}}} =
               Identity.validate(value)
    end
  end

  test "stored validation keeps legacy whitespace identities readable within the shared limit" do
    assert :ok = Identity.validate_stored("   ")
    assert :ok = Identity.validate_stored(String.duplicate(" ", 1_024))

    assert {:error,
            {:invalid_runtime_input_identity,
             %{field: :input_identity, limit_bytes: 1_024, reason: :too_large}}} =
             Identity.validate_stored(String.duplicate(" ", 1_025))
  end

  defp identity(size) do
    prefix = "https://inputs.example/"
    prefix <> String.duplicate("i", size - byte_size(prefix))
  end
end

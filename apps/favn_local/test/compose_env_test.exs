defmodule Favn.Dev.ComposeEnvTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.ComposeEnv

  test "literal encoding round-trips Compose-sensitive values" do
    environment = %{
      "DOLLARS" => "$HOME and ${MISSING}",
      "HASH_AND_QUOTES" => "# value with \"double\" and 'single' quotes",
      "MULTILINE" => "first line\nsecond line",
      "SLASHES" => "C:\\path\\before\\'quote"
    }

    assert {:ok, encoded} = ComposeEnv.encode(environment)
    assert encoded =~ "DOLLARS='$HOME and ${MISSING}'"
    assert encoded =~ "MULTILINE='first line\nsecond line'"
    assert encoded =~ "\\'single\\'"
    assert {:ok, ^environment} = ComposeEnv.decode(encoded)
  end

  test "encoding rejects invalid keys, values, and duplicate decoded keys" do
    assert {:error, :invalid_environment} = ComposeEnv.encode(%{"NOT-VALID" => "value"})
    assert {:error, :invalid_environment} = ComposeEnv.encode(%{"VALID" => "bad\0value"})

    assert {:error, :duplicate_environment_key} =
             ComposeEnv.decode("VALID='one'\nVALID='two'\n")
  end
end

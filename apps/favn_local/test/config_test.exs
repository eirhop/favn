defmodule FavnLocal.ConfigTest do
  use ExUnit.Case, async: true

  alias FavnLocal.Config

  @pin_key Base.encode64(String.duplicate("k", 32))

  test "requires the caller to provide PostgreSQL and runtime secrets" do
    assert {:error, {:missing_env, "FAVN_DATABASE_URL"}} = Config.load(env: %{})

    assert {:error, {:missing_env, "FAVN_RUNTIME_INPUT_PIN_KEY"}} =
             Config.load(env: %{"FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn"})
  end

  test "loads a Docker-free development configuration" do
    assert {:ok, config} =
             Config.load(
               root_dir: ".",
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
               }
             )

    assert config.workspace_id == "local-dev"
    assert config.orchestrator_port == 4101
    assert config.view_port == 4173
    assert config.runtime_input_pin_key == String.duplicate("k", 32)
    assert config.runner_release_id =~ ~r/^rr_[0-9a-f]{64}$/
  end

  test "rejects an invalid runtime input pin key" do
    assert {:error, {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEY", :invalid_key}} =
             Config.load(
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => "short"
               }
             )
  end

  test "reuses the local UI password across restarts" do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_local_config_#{System.unique_integer([:positive])}"
      )

    credentials = Path.join([root_dir, ".favn", "local", "credentials.json"])
    File.mkdir_p!(Path.dirname(credentials))
    File.write!(credentials, JSON.encode!(%{"view_password" => "stable-password"}))
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, config} =
             Config.load(
               root_dir: root_dir,
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
               }
             )

    assert config.bootstrap_password == "stable-password"
  end
end

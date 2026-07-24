defmodule FavnLocal.LocatorTest do
  use ExUnit.Case, async: true

  alias FavnLocal.Config
  alias FavnLocal.Locator

  setup do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_local_locator_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)

    {:ok, config} =
      Config.load(
        root_dir: root_dir,
        env: %{
          "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
          "FAVN_RUNTIME_INPUT_PIN_KEY" => Base.encode64(String.duplicate("k", 32))
        }
      )

    %{config: config, root_dir: root_dir}
  end

  test "writes only the small local locator and protects credentials", context do
    assert :ok = Locator.write(context.config, context.config.runner_release_id)
    assert {:ok, state} = Locator.read(context.root_dir)

    assert state.workspace_id == "local-dev"
    assert state.runner_release_id == context.config.runner_release_id

    credentials = Path.join([context.root_dir, ".favn", "local", "credentials.json"])
    assert {:ok, %{mode: mode}} = File.stat(credentials)
    assert Bitwise.band(mode, 0o777) == 0o600
  end

  test "refuses to silently reuse Docker-era generated state", context do
    legacy = Path.join([context.root_dir, ".favn", "compose", "compose.yml"])
    File.mkdir_p!(Path.dirname(legacy))
    File.write!(legacy, "legacy")

    assert {:error, {:legacy_local_state, path}} =
             Locator.write(context.config, context.config.runner_release_id)

    assert path == Path.join(context.root_dir, ".favn")
  end

  test "coexists with immutable manifest artifacts", context do
    artifact = Path.join([context.root_dir, ".favn", "dist", "manifest", "mv_test"])
    File.mkdir_p!(artifact)

    assert :ok = Locator.write(context.config, context.config.runner_release_id)
  end

  test "delete is idempotent", context do
    assert :ok = Locator.write(context.config, context.config.runner_release_id)
    assert :ok = Locator.delete(context.root_dir)
    assert :ok = Locator.delete(context.root_dir)
    assert {:error, :not_running} = Locator.read(context.root_dir)

    assert File.regular?(Path.join([context.root_dir, ".favn", "local", "credentials.json"]))
  end
end

defmodule FavnLocal.LocatorTest do
  use ExUnit.Case, async: false

  alias FavnLocal.Config
  alias FavnLocal.Distribution
  alias FavnLocal.Locator

  @local_distribution_address {127, 0, 0, 2}

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

    locator = Path.join([context.root_dir, ".favn", "local", "state.json"])
    credentials = Path.join([context.root_dir, ".favn", "local", "credentials.json"])
    credentials_data = credentials |> File.read!() |> JSON.decode!()

    refute File.read!(locator) =~ context.config.service_token
    assert credentials_data["service_token"] == context.config.service_token
    assert credentials_data["view_workspace_id"] == context.config.workspace_id
    assert credentials_data["view_username"] == "admin"
    assert credentials_data["view_password"] == context.config.bootstrap_password
    assert credentials_data["view_secret_key_base"] == context.config.view_secret_key_base

    if match?({:unix, _}, :os.type()) do
      assert {:ok, %{mode: mode}} = File.stat(credentials)
      assert Bitwise.band(mode, 0o777) == 0o600
    end
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

  test "an already distributed client installs the local resolver mapping", context do
    original_lookup = :inet_db.res_option(:lookup)
    started_distribution? = not Node.alive?()

    if started_distribution? do
      suffix = System.unique_integer([:positive, :monotonic])

      name =
        String.to_atom("favn_local_existing_client_#{suffix}@#{Distribution.local_host_alias()}")

      assert :ok = Distribution.start(name, "favn_local_existing_client_test")
    end

    on_exit(fn ->
      if started_distribution? and Node.alive?(), do: Node.stop()
      :ok = :inet_db.del_host(@local_distribution_address)
      :ok = :inet_db.set_lookup(original_lookup)
    end)

    assert Node.alive?()
    assert :ok = :inet_db.del_host(@local_distribution_address)
    assert :ok = Locator.write(context.config, context.config.runner_release_id)
    assert {:error, :not_running} = Locator.connect(context.root_dir)

    assert {:ok, @local_distribution_address} =
             :inet.getaddr(String.to_charlist(Distribution.local_host_alias()), :inet)
  end
end

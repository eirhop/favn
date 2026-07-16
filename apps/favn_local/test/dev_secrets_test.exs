defmodule Favn.Dev.SecretsTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config
  alias Favn.Dev.Paths
  alias Favn.Dev.Secrets

  setup do
    root_dir =
      Path.join(native_tmp_dir(), "favn_dev_secrets_test_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "generates stable project-local secrets instead of a shared cookie", %{root_dir: root_dir} do
    config = Config.resolve(root_dir: root_dir)

    assert {:ok, first} = Secrets.resolve(config, root_dir: root_dir)
    assert {:ok, second} = Secrets.resolve(config, root_dir: root_dir)

    assert first == second
    refute first["rpc_cookie"] == "FAVN_LOCAL_DEV_RPC_COOKIE"
    assert byte_size(first["service_token"]) >= 32
    assert byte_size(first["web_session_secret"]) >= 64

    if match?({:unix, _}, :os.type()) do
      assert {:ok, %{mode: mode}} = File.stat(Paths.secrets_path(root_dir))
      assert Bitwise.band(mode, 0o077) == 0
    end
  end

  test "configured HTTP secrets override persisted generated values", %{root_dir: root_dir} do
    config =
      Config.resolve(
        root_dir: root_dir,
        service_token: "configured-service-token",
        web_session_secret: String.duplicate("s", 64)
      )

    assert {:ok, secrets} = Secrets.resolve(config, root_dir: root_dir)
    assert secrets["service_token"] == "configured-service-token"
    assert secrets["web_session_secret"] == String.duplicate("s", 64)

    persisted = Paths.secrets_path(root_dir) |> File.read!()
    refute persisted =~ "configured-service-token"
    refute persisted =~ String.duplicate("s", 64)
  end


  defp native_tmp_dir do
    if match?({:unix, _}, :os.type()) and File.dir?("/tmp"), do: "/tmp", else: System.tmp_dir!()
  end
end

defmodule Favn.Dev.InstallScopeRegressionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Dev.Install
  alias Favn.Dev.State

  @moduletag :slow
  @moduletag timeout: 120_000

  @repo_root Path.expand("../../../..", __DIR__)

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_install_scope_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    File.write!(
      Path.join(root_dir, "mix.exs"),
      ~s|raise "asset install must not load the umbrella project"|
    )

    File.cp!(Path.join(@repo_root, "mix.lock"), Path.join(root_dir, "mix.lock"))

    for app <- ["favn_runner", "favn_orchestrator", "favn_view"] do
      app_root = Path.join([root_dir, "apps", app])
      File.mkdir_p!(app_root)

      File.write!(
        Path.join(app_root, "mix.exs"),
        ~s|raise "asset install must not load #{app}"|
      )
    end

    installer_root = Path.join(root_dir, "apps/favn_view/asset_installer")
    File.mkdir_p!(installer_root)

    File.cp!(
      Path.join(@repo_root, "apps/favn_view/asset_installer/mix.exs"),
      Path.join(installer_root, "mix.exs")
    )

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "cold install compiles only the asset installer dependency graph", %{root_dir: root_dir} do
    caller = self()

    deps_runner = fn _mix, ["deps.get"], opts ->
      runtime_deps = Path.join(Keyword.fetch!(opts, :cd), "deps")
      :ok = File.ln_s(Path.join(@repo_root, "deps"), runtime_deps)
      {"using resolved dependencies", 0}
    end

    output =
      capture_io(fn ->
        send(
          caller,
          {:install_result,
           Install.run(
             root_dir: root_dir,
             skip_tool_checks: true,
             runtime_deps_command_runner: deps_runner
           )}
        )
      end)

    assert_received {:install_result, {:ok, :installed}}
    assert output =~ "Favn install: resolving runtime dependencies"
    assert output =~ "Favn install: installing web asset binaries"

    assert {:ok, %{"materialized_root" => runtime_root}} =
             State.read_install_runtime(root_dir: root_dir)

    compiled_apps =
      runtime_root
      |> Path.join("_build/asset_installer/prod/lib")
      |> File.ls!()
      |> Enum.sort()

    assert compiled_apps == ["esbuild", "favn_view_asset_installer", "jason", "tailwind"]
    refute File.exists?(Path.join(runtime_root, "_build/dev"))

    started_at = System.monotonic_time(:millisecond)
    assert {:ok, :already_installed} = Install.run(root_dir: root_dir)
    assert System.monotonic_time(:millisecond) - started_at < 2_000
  end
end

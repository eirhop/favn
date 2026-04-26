defmodule Favn.Dev.RuntimeSourceTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RuntimeSource

  setup do
    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_runtime_source_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    %{tmp_dir: tmp_dir}
  end

  test "resolve_runtime_root/1 walks up to a valid runtime root", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "deps/favn")
    nested = Path.join(runtime_root, "apps/favn")

    create_runtime_root!(runtime_root)
    File.mkdir_p!(nested)

    assert {:ok, ^runtime_root} = RuntimeSource.resolve_runtime_root(nested)
  end

  test "resolve_runtime_root/1 returns not_found when no runtime root exists", %{tmp_dir: tmp_dir} do
    File.mkdir_p!(Path.join(tmp_dir, "a/b/c"))

    assert {:error, :not_found} = RuntimeSource.resolve_runtime_root(Path.join(tmp_dir, "a/b/c"))
  end

  test "fingerprint changes when copied runtime source content changes", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)

    source_file = Path.join(runtime_root, "apps/favn_local/lib/favn/dev/runtime_launch.ex")
    File.mkdir_p!(Path.dirname(source_file))
    File.write!(source_file, "defmodule RuntimeLaunch do\n  def value, do: :old\nend\n")

    assert {:ok, first} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    File.write!(source_file, "defmodule RuntimeLaunch do\n  def value, do: :new\nend\n")

    assert {:ok, second} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(first, ["runtime_source_tree", "sha256"]) !=
             get_in(second, ["runtime_source_tree", "sha256"])
  end

  test "fingerprint ignores generated dependency and build directories", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)

    assert {:ok, first} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner/_build/dev/lib/generated"))
    File.write!(Path.join(runtime_root, "apps/favn_runner/_build/dev/lib/generated/file"), "beam")
    File.mkdir_p!(Path.join(runtime_root, "web/favn_web/node_modules/vite"))
    File.write!(Path.join(runtime_root, "web/favn_web/node_modules/vite/index.js"), "generated")

    assert {:ok, second} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(first, ["runtime_source_tree", "sha256"]) ==
             get_in(second, ["runtime_source_tree", "sha256"])
  end

  defp create_runtime_root!(runtime_root) do
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(runtime_root, "web/favn_web"))

    File.write!(Path.join(runtime_root, "mix.lock"), "lock")

    File.write!(
      Path.join(runtime_root, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(runtime_root, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    File.write!(Path.join(runtime_root, "web/favn_web/package.json"), "{}")
  end
end

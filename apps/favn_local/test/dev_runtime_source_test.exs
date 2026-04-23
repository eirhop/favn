defmodule Favn.Dev.RuntimeSourceTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RuntimeSource

  test "resolve_runtime_root/1 walks up to a valid runtime root" do
    tmp_dir = Path.join(System.tmp_dir!(), "favn_runtime_source_test_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    runtime_root = Path.join(tmp_dir, "deps/favn")
    nested = Path.join(runtime_root, "apps/favn")

    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(runtime_root, "web/favn_web"))

    File.write!(Path.join(runtime_root, "apps/favn_runner/mix.exs"), "defmodule Runner.MixProject do end")

    File.write!(
      Path.join(runtime_root, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    File.write!(Path.join(runtime_root, "web/favn_web/package.json"), "{}")
    File.mkdir_p!(nested)

    assert {:ok, ^runtime_root} = RuntimeSource.resolve_runtime_root(nested)
  end

  test "resolve_runtime_root/1 returns not_found when no runtime root exists" do
    tmp_dir = Path.join(System.tmp_dir!(), "favn_runtime_source_missing_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    File.mkdir_p!(Path.join(tmp_dir, "a/b/c"))

    assert {:error, :not_found} = RuntimeSource.resolve_runtime_root(Path.join(tmp_dir, "a/b/c"))
  end
end

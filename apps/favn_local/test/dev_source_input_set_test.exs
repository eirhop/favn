defmodule Favn.Dev.Build.SourceInputSetTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Build.SourceInputSet

  setup do
    root = Path.join("/tmp", "favn_source_inputs_#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "lib"))
    File.mkdir_p!(Path.join(root, "priv"))
    File.mkdir_p!(Path.join(root, "config"))
    File.write!(Path.join(root, ".gitignore"), "deps/\nlib/generated/\npriv/cache/\n")
    File.write!(Path.join(root, "mix.exs"), "defmodule Fixture.MixProject do end\n")
    File.write!(Path.join(root, "lib/tracked.ex"), "defmodule Fixture.Tracked do end\n")
    File.write!(Path.join(root, "priv/resource.txt"), "resource")
    File.write!(Path.join(root, "config/runtime.exs"), "import Config\n")

    git!(root, ["init", "-q"])
    git!(root, ["add", "."])

    git!(root, [
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.com",
      "commit",
      "-qm",
      "initial"
    ])

    on_exit(fn -> File.rm_rf(root) end)
    %{root: root}
  end

  test "selects only the conventional application structure", %{root: root} do
    File.write!(Path.join(root, "lib/untracked.ex"), "defmodule Fixture.Untracked do end\n")

    for relative <- [
          "deploy/compose.yaml",
          "test/fixture.exs",
          "docs/guide.md",
          "notebooks/exploration.livemd",
          "local-data/database.db",
          ".elixir_ls/cache",
          "_build/cache"
        ] do
      path = Path.join(root, relative)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "unrelated")
    end

    unreadable = Path.join(root, "private-workspace")
    File.mkdir_p!(unreadable)
    File.write!(Path.join(unreadable, "secret"), "outside boundary")
    File.chmod!(unreadable, 0o000)
    on_exit(fn -> File.chmod(unreadable, 0o700) end)

    assert {:ok, input_set} = SourceInputSet.application(root, runtime_config: true)

    assert Enum.map(input_set.entries, & &1.path) == [
             "config/runtime.exs",
             "lib/tracked.ex",
             "lib/untracked.ex",
             "mix.exs",
             "priv/resource.txt"
           ]

    assert SourceInputSet.summary(input_set) == %{
             "declared_roots" => [
               "3rd_party",
               "CMakeLists.txt",
               "Cargo.lock",
               "Cargo.toml",
               "Makefile",
               "Makefile.win",
               "bin",
               "c_src",
               "checksum.exs",
               "config/runtime.exs",
               "include",
               "lib",
               "mix.exs",
               "mix.lock",
               "native",
               "priv",
               "rebar.config",
               "rebar.lock",
               "src"
             ],
             "file_count" => 5,
             "selection" => "git",
             "total_bytes" => Enum.sum(Enum.map(input_set.entries, & &1.size))
           }
  end

  test "Git-ignored files and symlinks inside allowed roots are not inputs", %{root: root} do
    generated = Path.join(root, "lib/generated")
    cache = Path.join(root, "priv/cache")
    File.mkdir_p!(generated)
    File.mkdir_p!(cache)
    File.write!(Path.join(generated, "cache.ex"), "one")
    File.write!(Path.join(cache, "target"), "cache")
    File.ln_s!("target", Path.join(cache, "libexample.so"))

    assert {:ok, before} = SourceInputSet.application(root, runtime_config: true)
    before_fingerprint = SourceInputSet.fingerprint(before)

    File.write!(Path.join(generated, "cache.ex"), "two")
    File.write!(Path.join(cache, "target"), "changed")

    assert {:ok, after_change} = SourceInputSet.application(root, runtime_config: true)
    assert SourceInputSet.fingerprint(after_change) == before_fingerprint

    destination = root <> "-copy"
    on_exit(fn -> File.rm_rf(destination) end)
    assert :ok = SourceInputSet.copy(after_change, destination)
    refute File.exists?(Path.join(destination, "lib/generated"))
    refute File.exists?(Path.join(destination, "priv/cache"))
  end

  test "non-Git sources use the same structural boundary", %{root: git_root} do
    root = git_root <> "-non-git"
    destination = root <> "-copy"
    on_exit(fn -> File.rm_rf(root) end)
    on_exit(fn -> File.rm_rf(destination) end)

    File.mkdir_p!(Path.join(root, "lib"))
    File.mkdir_p!(Path.join(root, "deploy"))
    File.write!(Path.join(root, "mix.exs"), "defmodule NonGit.MixProject do end\n")
    File.write!(Path.join(root, "lib/source.ex"), "defmodule NonGit.Source do end\n")
    File.write!(Path.join(root, "deploy/compose.yaml"), "not packaged")

    assert {:ok, input_set} = SourceInputSet.application(root)
    assert input_set.selection == :filesystem
    assert Enum.map(input_set.entries, & &1.path) == ["lib/source.ex", "mix.exs"]
    assert :ok = SourceInputSet.copy(input_set, destination)
    refute File.exists?(Path.join(destination, "deploy"))
  end

  test "dependencies ignored by a parent checkout use the structural boundary", %{root: root} do
    dependency = Path.join(root, "deps/local_dependency")
    File.mkdir_p!(Path.join(dependency, "lib"))
    File.mkdir_p!(Path.join(dependency, "docs"))
    File.write!(Path.join(dependency, "mix.exs"), "defmodule LocalDependency.MixProject do end\n")
    File.write!(Path.join(dependency, "lib/source.ex"), "defmodule LocalDependency do end\n")
    File.write!(Path.join(dependency, "docs/internal.md"), "outside boundary")

    assert {:ok, input_set} = SourceInputSet.application(dependency)
    assert input_set.selection == :filesystem
    assert Enum.map(input_set.entries, & &1.path) == ["lib/source.ex", "mix.exs"]
  end

  test "selects conventional native build descriptors and source roots", %{root: root} do
    File.write!(Path.join(root, "Makefile"), "all:\n\t@true\n")
    File.write!(Path.join(root, "checksum.exs"), "%{}\n")
    File.mkdir_p!(Path.join(root, "3rd_party/native_library"))
    File.write!(Path.join(root, "3rd_party/native_library/source.c"), "int main(void) {return 0;}")

    assert {:ok, input_set} = SourceInputSet.application(root)
    paths = Enum.map(input_set.entries, & &1.path)
    assert "Makefile" in paths
    assert "checksum.exs" in paths
    assert "3rd_party/native_library/source.c" in paths
  end

  test "selected symlinks and sensitive files remain rejected", %{root: root} do
    File.ln_s!("tracked.ex", Path.join(root, "lib/selected.so"))

    assert {:error, {:symlink_not_supported, "lib/selected.so"}} =
             SourceInputSet.application(root)

    File.rm!(Path.join(root, "lib/selected.so"))
    File.write!(Path.join(root, "priv/credentials.json"), ~s({"token":"secret"}))

    assert {:error, {:sensitive_source_file, "priv/credentials.json"}} =
             SourceInputSet.application(root)
  end

  test "copying fails when a selected input changes after planning", %{root: root} do
    assert {:ok, input_set} = SourceInputSet.application(root)
    File.write!(Path.join(root, "lib/tracked.ex"), "defmodule Fixture.Changed do end\n")

    assert {:error, {:source_input_changed, "lib/tracked.ex"}} =
             SourceInputSet.copy(input_set, root <> "-changed-copy")
  end

  defp git!(root, args) do
    assert {output, 0} = System.cmd("git", ["-C", root | args], stderr_to_stdout: true)
    String.trim(output)
  end
end

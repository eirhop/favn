defmodule Favn.Dev.RuntimeSourceTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RuntimeSource
  alias Favn.Dev.RuntimeTreePolicy
  alias Favn.Dev.RuntimeWorkspace

  @git System.find_executable("git")

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

  test "fingerprint ignores generated directories skipped during materialization", %{
    tmp_dir: tmp_dir
  } do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)

    assert {:ok, first} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner/_build/dev/lib/generated"))
    File.write!(Path.join(runtime_root, "apps/favn_runner/_build/dev/lib/generated/file"), "beam")
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner/deps/generated"))
    File.write!(Path.join(runtime_root, "apps/favn_runner/deps/generated/file"), "dep")
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner/doc/generated"))
    File.write!(Path.join(runtime_root, "apps/favn_runner/doc/generated/index.html"), "docs")
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner/tmp/generated"))
    File.write!(Path.join(runtime_root, "apps/favn_runner/tmp/generated/file"), "temporary")
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_view/priv/static/assets/js"))
    File.write!(Path.join(runtime_root, "apps/favn_view/priv/static/assets/js/app.js"), "built")

    assert {:ok, second} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(first, ["runtime_source_tree", "sha256"]) ==
             get_in(second, ["runtime_source_tree", "sha256"])
  end

  test "fingerprint metadata reports the shared runtime tree policy", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)

    assert {:ok, fingerprint} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(fingerprint, ["runtime_source_tree", "entries"]) == RuntimeTreePolicy.entries()

    assert get_in(fingerprint, ["runtime_source_tree", "ignored_entries"]) ==
             Enum.sort(RuntimeTreePolicy.ignored_entries())
  end

  test "Git sources hash only dirty files on top of the committed tree", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)
    commit_runtime_root!(runtime_root)

    assert {:ok, clean} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(clean, ["runtime_source_tree", "strategy"]) == "git_tree"
    assert get_in(clean, ["runtime_source_tree", "changed_path_count"]) == 0

    source_file = Path.join(runtime_root, "apps/favn_local/lib/favn/dev/runtime_launch.ex")
    File.mkdir_p!(Path.dirname(source_file))
    File.write!(source_file, "defmodule RuntimeLaunch do\nend\n")

    assert {:ok, dirty} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(dirty, ["runtime_source_tree", "strategy"]) == "git_worktree"
    assert get_in(dirty, ["runtime_source_tree", "changed_path_count"]) == 1

    refute get_in(dirty, ["runtime_source_tree", "sha256"]) ==
             get_in(clean, ["runtime_source_tree", "sha256"])
  end

  test "dirty Git fingerprints handle tracked changes and deletions", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)

    source_file = Path.join(runtime_root, "apps/favn_local/lib/favn/dev/runtime_launch.ex")
    File.mkdir_p!(Path.dirname(source_file))
    File.write!(source_file, "defmodule RuntimeLaunch do\nend\n")
    commit_runtime_root!(runtime_root)

    assert {:ok, clean} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    File.write!(source_file, "defmodule RuntimeLaunch.Changed do\nend\n")

    assert {:ok, modified} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert {_, 0} =
             System.cmd(@git, ["add", Path.relative_to(source_file, runtime_root)],
               cd: runtime_root
             )

    assert {:ok, staged} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    File.rm!(source_file)

    assert {:ok, deleted} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(modified, ["runtime_source_tree", "strategy"]) == "git_worktree"
    assert get_in(deleted, ["runtime_source_tree", "strategy"]) == "git_worktree"
    assert staged["runtime_source_tree"] == modified["runtime_source_tree"]

    hashes =
      for fingerprint <- [clean, modified, deleted] do
        get_in(fingerprint, ["runtime_source_tree", "sha256"])
      end

    assert length(Enum.uniq(hashes)) == 3
  end

  test "Git fingerprints ignore generated working-tree files", %{tmp_dir: tmp_dir} do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)
    commit_runtime_root!(runtime_root)

    assert {:ok, clean} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    generated = Path.join(runtime_root, "apps/favn_runner/_build/dev/generated.beam")
    File.mkdir_p!(Path.dirname(generated))
    File.write!(generated, "generated")

    assert {:ok, with_generated} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert with_generated["runtime_source_tree"] == clean["runtime_source_tree"]
  end

  test "Git fingerprints support runtime roots nested inside a repository", %{tmp_dir: tmp_dir} do
    git_root = Path.join(tmp_dir, "repository")
    runtime_root = Path.join(git_root, "vendor/favn")
    create_runtime_root!(runtime_root)
    commit_runtime_root!(git_root)

    source_file = Path.join(runtime_root, "apps/favn_runner/lib/nested_source.ex")
    File.mkdir_p!(Path.dirname(source_file))
    File.write!(source_file, "defmodule NestedSource do\nend\n")

    assert {:ok, fingerprint} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(fingerprint, ["runtime_source_tree", "strategy"]) == "git_worktree"
    assert get_in(fingerprint, ["runtime_source_tree", "changed_path_count"]) == 1
  end

  test "Git fingerprints include ignored source directories that are materialized", %{
    tmp_dir: tmp_dir
  } do
    runtime_root = Path.join(tmp_dir, "favn")
    create_runtime_root!(runtime_root)
    File.write!(Path.join(runtime_root, ".gitignore"), "/config/private/\n")
    commit_runtime_root!(runtime_root)

    assert {:ok, clean} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    private_file = Path.join(runtime_root, "config/private/runtime.exs")
    File.mkdir_p!(Path.dirname(private_file))
    File.write!(private_file, "import Config\n")

    assert {:ok, dirty} =
             RuntimeSource.fingerprint(%{kind: :dependency_checkout, root: runtime_root})

    assert get_in(dirty, ["runtime_source_tree", "strategy"]) == "git_worktree"
    assert get_in(dirty, ["runtime_source_tree", "changed_path_count"]) == 1

    assert get_in(dirty, ["runtime_source_tree", "file_count"]) ==
             get_in(clean, ["runtime_source_tree", "file_count"]) + 1

    refute get_in(dirty, ["runtime_source_tree", "sha256"]) ==
             get_in(clean, ["runtime_source_tree", "sha256"])
  end

  test "fingerprinting and materialization reject symlinks outside the runtime root", %{
    tmp_dir: tmp_dir
  } do
    runtime_root = Path.join(tmp_dir, "favn")
    consumer_root = Path.join(tmp_dir, "consumer")
    create_runtime_root!(runtime_root)

    external_file = Path.join(tmp_dir, "external.ex")
    symlink = Path.join(runtime_root, "apps/favn_runner/lib/external.ex")
    File.write!(external_file, "defmodule External do\nend\n")
    File.mkdir_p!(Path.dirname(symlink))
    File.ln_s!(external_file, symlink)

    source = %{kind: :dependency_checkout, root: runtime_root}

    assert {:error, {:unsupported_runtime_entry, ^symlink, :symlink}} =
             RuntimeSource.fingerprint(source)

    assert {:error, {:unsupported_runtime_entry, ^symlink, :symlink}} =
             RuntimeWorkspace.materialize(source, root_dir: consumer_root)

    refute File.exists?(
             Path.join(
               consumer_root,
               ".favn/install/runtime_root/apps/favn_runner/lib/external.ex"
             )
           )

    commit_runtime_root!(runtime_root)

    assert {:error, {:unsupported_runtime_entry, ^symlink, :symlink}} =
             RuntimeSource.fingerprint(source)
  end

  defp create_runtime_root!(runtime_root) do
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_runner"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_view"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_view/priv"))
    File.mkdir_p!(Path.join(runtime_root, "apps/favn_view/priv/static"))

    File.write!(Path.join(runtime_root, "mix.lock"), "lock")

    File.write!(
      Path.join(runtime_root, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(runtime_root, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    File.write!(
      Path.join(runtime_root, "apps/favn_view/mix.exs"),
      "defmodule View.MixProject do end"
    )
  end

  defp commit_runtime_root!(runtime_root) do
    assert is_binary(@git)
    assert {_, 0} = System.cmd(@git, ["init", "-q"], cd: runtime_root)
    assert {_, 0} = System.cmd(@git, ["add", "."], cd: runtime_root)

    assert {_, 0} =
             System.cmd(
               @git,
               [
                 "-c",
                 "user.name=Test",
                 "-c",
                 "user.email=test@example.com",
                 "commit",
                 "-qm",
                 "fixture"
               ],
               cd: runtime_root
             )
  end
end

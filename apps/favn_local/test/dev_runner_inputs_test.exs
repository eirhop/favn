defmodule Favn.Dev.Build.RunnerInputsTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Build.RunnerInputs
  alias Favn.Dev.Maintainer.{RunnerBuildCapability, Source}

  setup do
    root = Path.join("/tmp", "favn_runner_inputs_git_#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "apps/favn_core/lib"))
    File.mkdir_p!(Path.join(root, "scripts"))
    File.write!(Path.join(root, "mix.exs"), "defmodule Fixture.MixProject do end\n")
    File.write!(Path.join(root, "apps/favn_core/mix.exs"), "defmodule Core.MixProject do end\n")
    File.write!(Path.join(root, "apps/favn_core/lib/tracked.ex"), "initial")
    File.write!(Path.join(root, "scripts/control_plane_build_id.exs"), ":ok\n")

    File.write!(
      Path.join(root, ".gitignore"),
      "apps/favn_core/lib/generated/\nignored.log\n"
    )

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

  test "requires a detached, clean Favn checkout", %{root: root} do
    assert {:error, :favn_checkout_not_pinned} = RunnerInputs.verify_favn_checkout(root)

    git!(root, ["checkout", "--detach", "-q"])
    assert {:ok, revision} = RunnerInputs.verify_favn_checkout(root)
    assert revision =~ ~r/\A[0-9a-f]{40}\z/

    untracked = Path.join(root, "apps/favn_core/lib/untracked.ex")
    File.write!(untracked, "defmodule Untracked do end")
    assert {:error, :favn_checkout_not_pinned} = RunnerInputs.verify_favn_checkout(root)
    File.rm!(untracked)

    File.write!(Path.join(root, "apps/favn_core/lib/tracked.ex"), "modified")
    assert {:error, :favn_checkout_not_pinned} = RunnerInputs.verify_favn_checkout(root)
  end

  test "maintainer capability permits the selected branch and dirty state only", %{root: root} do
    revision = git!(root, ["rev-parse", "HEAD"])
    {:ok, clean_fingerprint} = Source.fingerprint(root)

    clean = %RunnerBuildCapability{
      consumer_root: root,
      checkout: root,
      revision: revision,
      dirty: false,
      fingerprint: clean_fingerprint
    }

    assert {:ok, ^revision} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: clean)

    cache = Path.join(root, "apps/favn_core/lib/generated")
    File.mkdir_p!(cache)
    File.write!(Path.join(cache, "cache"), "one")
    File.ln_s!("cache", Path.join(cache, "libexample.so"))
    File.write!(Path.join(root, "ignored.log"), "one")

    assert {:ok, ^revision} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: clean)

    File.write!(Path.join(cache, "cache"), "two")
    File.write!(Path.join(root, "ignored.log"), "two")

    assert {:ok, ^revision} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: clean)

    File.write!(Path.join(root, "apps/favn_core/lib/tracked.ex"), "modified")

    assert {:error, :favn_maintainer_checkout_changed} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: clean)

    {:ok, dirty_fingerprint} = Source.fingerprint(root)
    dirty = %{clean | dirty: true, fingerprint: dirty_fingerprint}

    assert {:ok, ^revision} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: dirty)

    nested_selected_input =
      Path.join([root, "apps", "favn_core", "lib", "test", "selected.ex"])

    File.mkdir_p!(Path.dirname(nested_selected_input))
    File.write!(nested_selected_input, "defmodule Selected, do: nil\n")

    assert {:error, :favn_maintainer_checkout_changed} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: dirty)

    {:ok, changed_fingerprint} = Source.fingerprint(root)
    changed_dirty = %{dirty | fingerprint: changed_fingerprint}

    assert {:ok, ^revision} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: changed_dirty)

    wrong_revision = %{changed_dirty | revision: String.duplicate("f", 40)}

    assert {:error, :favn_maintainer_checkout_changed} =
             RunnerInputs.verify_favn_checkout(root, maintainer_runner_build: wrong_revision)
  end

  defp git!(root, args) do
    assert {output, 0} = System.cmd("git", ["-C", root | args], stderr_to_stdout: true)
    String.trim(output)
  end
end

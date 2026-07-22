defmodule Favn.Dev.Build.RunnerInputsTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Build.RunnerInputs

  setup do
    root = Path.join("/tmp", "favn_runner_inputs_git_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    File.write!(Path.join(root, "tracked"), "initial")

    git!(root, ["init", "-q"])
    git!(root, ["add", "tracked"])

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

    File.write!(Path.join(root, "untracked.ex"), "defmodule Untracked do end")
    assert {:error, :favn_checkout_not_pinned} = RunnerInputs.verify_favn_checkout(root)
    File.rm!(Path.join(root, "untracked.ex"))

    File.write!(Path.join(root, "tracked"), "modified")
    assert {:error, :favn_checkout_not_pinned} = RunnerInputs.verify_favn_checkout(root)
  end

  defp git!(root, args) do
    assert {_output, 0} = System.cmd("git", ["-C", root | args], stderr_to_stdout: true)
  end
end

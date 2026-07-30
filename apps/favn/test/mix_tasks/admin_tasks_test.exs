defmodule Mix.Tasks.Favn.AdminTasksTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.AdminSecret
  alias Mix.Tasks.Favn.Admin.Bootstrap
  alias Mix.Tasks.Favn.Admin.Recover

  test "bootstrap requires explicit workspaces and never accepts a password argument" do
    assert {:error, "at least one --workspace is required"} =
             Bootstrap.parse_args(["--username", "admin"])

    assert {:error, message} =
             Bootstrap.parse_args([
               "--workspace",
               "ws_one",
               "--username",
               "admin",
               "--password",
               "must-never-enter-process-arguments"
             ])

    assert message =~ "invalid option"

    assert {:ok, input, []} =
             Bootstrap.parse_args([
               "--workspace",
               "ws_two",
               "--workspace",
               "ws_one",
               "--username",
               "admin"
             ])

    assert input.workspace_ids == ["ws_two", "ws_one"]
    assert input.display_name == "admin"
  end

  test "recovery accepts only username and optional password file" do
    assert {:error, "--username is required"} = Recover.parse_args([])

    assert {:ok, %{username: "admin"}, [password_file: "/run/secrets/admin"]} =
             Recover.parse_args([
               "--username",
               "admin",
               "--password-file",
               "/run/secrets/admin"
             ])
  end

  @tag :tmp_dir
  test "password files require a regular protected Unix file", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "password")
    File.write!(path, "a-password-that-is-long-enough\n")

    case :os.type() do
      {:unix, _name} ->
        File.chmod!(path, 0o600)
        assert {:ok, "a-password-that-is-long-enough"} = AdminSecret.read_protected_file(path)

        File.chmod!(path, 0o640)
        assert {:error, message} = AdminSecret.read_protected_file(path)
        assert message =~ "group/other"

      {:win32, _name} ->
        assert {:error, message} = AdminSecret.read_protected_file(path)
        assert message =~ "use stdin on Windows"
    end
  end
end

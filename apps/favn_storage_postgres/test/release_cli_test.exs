defmodule FavnStoragePostgres.ReleaseCLITest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias FavnStoragePostgres.ReleaseCLI

  defmodule FakeRelease do
    def migrate, do: ok(:migrate)
    def verify_schema, do: ok(:verify_schema)
    def verify_restore, do: ok(:verify_restore)
    def grant_runtime, do: ok(:grant_runtime)
    def runtime_input_key_inventory, do: ok(:runtime_input_key_inventory)
    def preflight_upgrade, do: ok(:preflight_upgrade)

    def provision_workspace(workspace),
      do: {:ok, Map.merge(%{operation: :provision_workspace, status: :ok}, workspace)}

    def compact_runtime_input_keys(versions),
      do:
        {:ok,
         %{
           operation: :compact_runtime_input_keys,
           status: :ok,
           requested_versions: versions
         }}

    defp ok(operation), do: {:ok, %{operation: operation, status: :ok}}
  end

  defmodule FailingRelease do
    def verify_schema,
      do: {:error, %{operation: :verify_schema, status: :error, code: :schema_not_ready}}
  end

  test "dispatches fixed no-argument operations" do
    output =
      capture_io(fn ->
        assert :ok = ReleaseCLI.run!(:migrate, %{}, FakeRelease)
      end)

    assert output =~ "operation=migrate status=ok"
  end

  test "reads workspace and key versions from environment instead of arguments" do
    workspace_env = %{
      "FAVN_WORKSPACE_ID" => "workspace-1",
      "FAVN_WORKSPACE_SLUG" => "workspace-one",
      "FAVN_WORKSPACE_NAME" => "Workspace One"
    }

    workspace_output =
      capture_io(fn ->
        assert :ok = ReleaseCLI.run!(:provision_workspace, workspace_env, FakeRelease)
      end)

    assert workspace_output =~ "workspace-1"
    assert workspace_output =~ "Workspace One"

    key_output =
      capture_io(fn ->
        assert :ok =
                 ReleaseCLI.run!(
                   :compact_runtime_input_keys,
                   %{"FAVN_RUNTIME_INPUT_KEY_VERSIONS" => "3,1,3"},
                   FakeRelease
                 )
      end)

    assert key_output =~ "requested_versions: [3, 1]"
  end

  test "raises bounded operation codes without leaking failure details" do
    error =
      assert_raise RuntimeError, fn ->
        ReleaseCLI.run!(:verify_schema, %{}, FailingRelease)
      end

    assert error.message == "release operation verify_schema failed: schema_not_ready"

    missing =
      assert_raise RuntimeError, fn ->
        ReleaseCLI.run!(:provision_workspace, %{}, FakeRelease)
      end

    assert missing.message ==
             "release operation provision_workspace failed: missing_or_invalid_environment"
  end
end

defmodule FavnStoragePostgres.ReleaseCLITest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias FavnStoragePostgres.Bootstrap.Result
  alias FavnStoragePostgres.ReleaseCLI

  defmodule FakeRelease do
    alias FavnStoragePostgres.Bootstrap.Result

    def bootstrap(_env), do: Result.ready(:bootstrap, [:database, :runtime_verification])
    def database_status(_env), do: Result.status(:status, :schema_upgrade_required, :migrations)

    def upgrade(_env),
      do: Result.error(:upgrade, :operation_in_progress, :operation_in_progress, :lock)

    def migrate, do: ok(:migrate)
    def verify_schema, do: ok(:verify_schema)
    def verify_restore, do: ok(:verify_restore)
    def grant_runtime, do: ok(:grant_runtime)
    def runtime_input_key_inventory, do: ok(:runtime_input_key_inventory)

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

  test "emits one JSON document and stable exit codes for composite operations" do
    {0, bootstrap} = run(:bootstrap)
    assert bootstrap["contract_version"] == 1
    assert bootstrap["state"] == "ready"
    assert bootstrap["runtime_verified"]
    assert is_binary(bootstrap["release"]["favn_version"])
    assert is_integer(bootstrap["release"]["latest_migration_version"])

    {2, status} = run(:status)
    assert status["outcome"] == "changes_required"
    assert status["state"] == "schema_upgrade_required"
    assert status["safe_to_retry"]

    {75, upgrade} = run(:upgrade)
    assert upgrade["state"] == "operation_in_progress"
    assert upgrade["code"] == "operation_in_progress"
  end

  test "reads workspace and key versions from environment instead of arguments" do
    workspace_env = %{
      "FAVN_WORKSPACE_ID" => "workspace-1",
      "FAVN_WORKSPACE_SLUG" => "workspace-one",
      "FAVN_WORKSPACE_NAME" => "Workspace One"
    }

    {0, workspace} = run(:provision_workspace, workspace_env)
    assert workspace["workspace_id"] == "workspace-1"
    assert workspace["display_name"] == "Workspace One"

    {0, compaction} =
      run(:compact_runtime_input_keys, %{"FAVN_RUNTIME_INPUT_KEY_VERSIONS" => "3,1,3"})

    assert compaction["requested_versions"] == [3, 1]
  end

  test "uses bounded legacy failure output without leaking returned details" do
    {70, failure} = run(:verify_schema, %{}, FailingRelease)

    assert failure == %{
             "code" => "schema_not_ready",
             "operation" => "verify_schema",
             "status" => "error"
           }

    {70, missing} = run(:provision_workspace)
    assert missing["code"] == "missing_or_invalid_environment"
    refute inspect(missing) =~ "credential"
  end

  test "result contract reserves unknown outcome as unsafe to retry" do
    {:error, result} =
      Result.error(:bootstrap, :unknown_outcome, :unknown_outcome, :identity_mapping)

    refute result.safe_to_retry
    assert Result.exit_code(result) == 76

    {:error, unavailable} =
      Result.error(
        :status,
        :authentication_unavailable,
        :authentication_unavailable,
        :runtime_connection
      )

    assert Result.exit_code(unavailable) == 69
  end

  test "release entrypoint keeps logs off the JSON stdout stream" do
    suffix = System.unique_integer([:positive])
    stdout_path = Path.join(System.tmp_dir!(), "favn-release-stdout-#{suffix}")
    stderr_path = Path.join(System.tmp_dir!(), "favn-release-stderr-#{suffix}")
    on_exit(fn -> Enum.each([stdout_path, stderr_path], &File.rm/1) end)

    expression = """
    defmodule FavnReleaseCLILoggingFixture do
      require Logger
      def bootstrap(_env) do
        Logger.warning("bootstrap progress belongs on stderr")
        FavnStoragePostgres.Bootstrap.Result.ready(:bootstrap, [:runtime_verification])
      end
    end
    FavnStoragePostgres.ReleaseCLI.run!(:bootstrap, %{}, FavnReleaseCLILoggingFixture)
    """

    script =
      ~S(MIX_ENV=test "$1" run --no-compile --no-start -e "$FAVN_RELEASE_EXPR" >"$2" 2>"$3")

    mix = System.find_executable("mix") || raise "mix executable is required"

    assert {_output, 0} =
             System.cmd(
               "sh",
               [
                 "-c",
                 script,
                 "favn-release-cli",
                 mix,
                 stdout_path,
                 stderr_path
               ],
               env: [{"FAVN_RELEASE_EXPR", expression}]
             )

    stdout = File.read!(stdout_path)
    stderr = File.read!(stderr_path)
    assert [json] = String.split(stdout, "\n", trim: true)
    assert Jason.decode!(json)["state"] == "ready"
    assert stderr =~ "bootstrap progress belongs on stderr"
    assert stderr =~ "favn.release operation=bootstrap state=ready exit=0"
  end

  defp run(operation, env \\ %{}, release \\ FakeRelease) do
    key = {__MODULE__, make_ref()}
    parent = self()

    stderr =
      capture_io(:stderr, fn ->
        output =
          capture_io(fn ->
            exit_code = ReleaseCLI.run(operation, env, release)
            send(parent, {key, exit_code})
          end)

        send(parent, {key, output})
      end)

    assert_receive {^key, exit_code}
    assert_receive {^key, output}
    assert [json] = String.split(output, "\n", trim: true)
    assert [_summary] = String.split(stderr, "\n", trim: true)
    {exit_code, Jason.decode!(json)}
  end
end

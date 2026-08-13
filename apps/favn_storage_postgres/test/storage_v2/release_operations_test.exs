defmodule FavnStoragePostgres.StorageV2.ReleaseOperationsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FavnStoragePostgres.Release
  alias FavnStoragePostgres.Repo

  setup do
    _url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    previous =
      Map.new(
        [
          "FAVN_DATABASE_AUTH_MODE",
          "FAVN_DATABASE_SSL_MODE",
          "FAVN_DATABASE_RUNTIME_ROLE",
          "FAVN_DEPLOYMENT_MODE",
          "FAVN_RUNTIME_INPUT_PIN_KEYS",
          "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION"
        ],
        &{&1, System.get_env(&1)}
      )

    System.put_env("FAVN_DATABASE_AUTH_MODE", "password")
    System.put_env("FAVN_DATABASE_SSL_MODE", "disable")
    System.put_env("FAVN_DEPLOYMENT_MODE", "production")

    System.put_env(
      "FAVN_RUNTIME_INPUT_PIN_KEYS",
      JSON.encode!(%{
        "1" => "0123456789abcdef0123456789abcdef",
        "2" => "abcdef0123456789abcdef0123456789"
      })
    )

    System.put_env("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "2")

    on_exit(fn ->
      Enum.each(previous, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)

    :ok
  end

  test "one-off operations own and stop their validated Repo" do
    database_url = System.fetch_env!("FAVN_DATABASE_URL")
    unique = System.unique_integer([:positive])
    workspace_id = "release-operation-#{unique}"

    object_suffix =
      unique |> :erlang.phash2(0xFFFFFFFF) |> Integer.to_string(16) |> String.pad_leading(12, "0")

    telemetry_handler = "release-operation-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        telemetry_handler,
        [
          [:favn, :storage_postgres, :release_operation, :start],
          [:favn, :storage_postgres, :release_operation, :stop]
        ],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(telemetry_handler) end)

    connection_options =
      database_url
      |> Ecto.Repo.Supervisor.parse_url()
      |> Keyword.put(:ssl, false)

    connection =
      start_supervised!({Postgrex, connection_options},
        id: :release_operation_setup_connection
      )

    Postgrex.query!(connection, "CREATE SCHEMA IF NOT EXISTS favn_control", [])

    log =
      capture_log(fn ->
        assert {:error, %{operation: :migrate, status: :error, code: :unsafe_migrator_authority}} =
                 Release.migrate()

        refute Process.whereis(Repo)

        assert {:ok,
                %{
                  operation: :verify_schema,
                  status: :ok,
                  schema: "favn_control",
                  definition_fingerprint: fingerprint
                }} = Release.verify_schema()

        assert byte_size(fingerprint) == 64
        refute Process.whereis(Repo)

        assert {:error,
                %{
                  operation: :verify_workspace,
                  status: :error,
                  code: :workspace_not_found,
                  workspace_id: ^workspace_id
                }} = Release.verify_workspace(workspace_id)

        workspace = %{
          "operation_id" => "release-operation-#{workspace_id}",
          "workspace" => %{
            "id" => workspace_id,
            "slug" => workspace_id,
            "display_name" => "Release operation test"
          },
          "administrator" => %{
            "mode" => "entra",
            "username" => "admin-#{workspace_id}",
            "display_name" => "Release administrator",
            "tenant_id" => "11111111-1111-1111-1111-111111111111",
            "object_id" => "22222222-2222-4222-8222-#{object_suffix}"
          }
        }

        provisioning_env =
          System.get_env()
          |> Map.put("FAVN_OPERATOR_COMMAND_HMAC_SECRET", "release-operation-hmac-secret-0001")

        assert {:ok, %{operation: :provision_workspace, status: :ok, workspace_id: ^workspace_id}} =
                 Release.provision_workspace_administrator(workspace, provisioning_env)

        assert {:ok, %{operation: :workspace_status, state: :ready, workspace_id: ^workspace_id}} =
                 Release.workspace_status(workspace_id, provisioning_env)

        assert {:ok, %{operation: :provision_workspace, status: :ok, workspace_id: ^workspace_id}} =
                 Release.provision_workspace_administrator(workspace, provisioning_env)

        assert {:ok, %{operation: :verify_restore, status: :ok, statement_timeout_ms: 600_000}} =
                 Release.verify_restore()

        Postgrex.query!(
          connection,
          """
          INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at)
          VALUES (98, clock_timestamp())
          ON CONFLICT (key_version) DO NOTHING
          """,
          []
        )

        assert {:ok,
                %{
                  operation: :runtime_input_key_inventory,
                  status: :ok,
                  current_version: 2,
                  retained_versions: [1, 2],
                  invalid_versions: [],
                  inventory: inventory
                }} = Release.runtime_input_key_inventory()

        assert Enum.any?(inventory, &(&1.key_version == 98 and &1.pin_count == 0))

        assert {:error,
                %{
                  operation: :compact_runtime_input_keys,
                  status: :error,
                  code: :current_key_version_requested,
                  current_version: 2
                }} = Release.compact_runtime_input_keys(2)

        assert {:ok,
                %{
                  operation: :compact_runtime_input_keys,
                  status: :ok,
                  requested_versions: [98],
                  removed_versions: [98]
                }} = Release.compact_runtime_input_keys(98)

        System.delete_env("FAVN_DEPLOYMENT_MODE")

        assert {:error, %{operation: :migrate, status: :error, code: :unsafe_migrator_authority}} =
                 Release.migrate()

        assert {:error,
                %{
                  operation: :grant_runtime,
                  status: :error,
                  code: :unsafe_migrator_authority
                }} = Release.grant_runtime()

        System.put_env("FAVN_DEPLOYMENT_MODE", "production")

        %{rows: [[current_role]]} = Postgrex.query!(connection, "SELECT current_user", [])
        System.put_env("FAVN_DATABASE_RUNTIME_ROLE", current_role)

        assert {:error, %{operation: :migrate, status: :error, code: :restricted_runtime_role}} =
                 Release.migrate()

        assert {:error,
                %{operation: :grant_runtime, status: :error, code: :restricted_runtime_role}} =
                 Release.grant_runtime()

        refute Process.whereis(Repo)
      end)

    cleanup_provisioned_administrator!(connection, workspace_id)

    refute log =~ database_url

    if database_userinfo = URI.parse(database_url).userinfo do
      refute log =~ database_userinfo
    end

    assert_receive {[:favn, :storage_postgres, :release_operation, :start],
                    %{system_time: system_time}, %{operation: :verify_schema}}

    assert is_integer(system_time)

    assert_receive {[:favn, :storage_postgres, :release_operation, :stop],
                    %{duration_ms: duration_ms}, %{operation: :verify_schema, status: :ok}}

    assert is_integer(duration_ms) and duration_ms >= 0

    assert_receive {[:favn, :storage_postgres, :release_operation, :stop],
                    %{duration_ms: failed_duration_ms},
                    %{operation: :migrate, status: :error, code: :restricted_runtime_role}}

    assert is_integer(failed_duration_ms) and failed_duration_ms >= 0
  end

  defp cleanup_provisioned_administrator!(connection, workspace_id) do
    %{rows: [[operation_id, actor_id]]} =
      Postgrex.query!(
        connection,
        """
        SELECT operation_id, actor_id
        FROM favn_control.workspace_provisioning_operations
        WHERE workspace_id = $1
        """,
        [workspace_id]
      )

    statements = [
      {"DELETE FROM favn_control.workspace_provisioning_operations WHERE operation_id = $1",
       [operation_id]},
      {"DELETE FROM favn_control.auth_audit_entries WHERE command_id = $1", [operation_id]},
      {"DELETE FROM favn_control.auth_platform_audit_entries WHERE command_id = $1",
       [operation_id]},
      {"DELETE FROM favn_control.auth_external_identities WHERE actor_id = $1", [actor_id]},
      {"DELETE FROM favn_control.auth_workspace_memberships WHERE actor_id = $1", [actor_id]},
      {"DELETE FROM favn_control.auth_platform_grants WHERE actor_id = $1", [actor_id]},
      {"DELETE FROM favn_control.auth_actors WHERE actor_id = $1", [actor_id]}
    ]

    Enum.each(statements, fn {statement, params} ->
      Postgrex.query!(connection, statement, params)
    end)
  end
end

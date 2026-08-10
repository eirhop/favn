defmodule FavnStoragePostgres.Bootstrap.ConnectionTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Bootstrap
  alias FavnStoragePostgres.Bootstrap.{Config, Connection, Profile}

  @credentials_supervisor FavnStoragePostgres.BootstrapMaintenanceAuth.CredentialsSupervisor

  setup do
    previous_environment = Application.get_env(:favn_storage_postgres, :environment)
    Application.put_env(:favn_storage_postgres, :environment, :test)

    on_exit(fn ->
      if is_nil(previous_environment) do
        Application.delete_env(:favn_storage_postgres, :environment)
      else
        Application.put_env(:favn_storage_postgres, :environment, previous_environment)
      end
    end)
  end

  test "a local authentication lifecycle name collision has a bounded classification" do
    assert {:ok, occupied} = Agent.start_link(fn -> :occupied end, name: @credentials_supervisor)
    on_exit(fn -> if Process.alive?(occupied), do: Agent.stop(occupied) end)

    profile = %Profile{
      purpose: :bootstrap,
      authentication_mode: :azure_managed_identity,
      role: "favn_bootstrap",
      source: {:azure, "127.0.0.1", 5432, "11111111-1111-1111-1111-111111111111"},
      base_env: %{
        "FAVN_DEPLOYMENT_MODE" => "production",
        "FAVN_DATABASE_SSL_MODE" => "disable"
      }
    }

    config = %Config{
      operation: :status,
      target_database: "favn",
      maintenance_database: "postgres",
      bootstrap: profile,
      migrator: profile,
      runtime: profile,
      workspace: nil
    }

    assert {:error, :authentication_lifecycle_conflict} =
             Connection.with_raw(config, profile, "postgres", :bootstrap_maintenance, fn _conn ->
               flunk("connection callback must not run after a lifecycle-start conflict")
             end)
  end

  test "workflow diagnostics distinguish a lifecycle conflict from a token-provider outage" do
    assert {:ok, occupied} = Agent.start_link(fn -> :occupied end, name: @credentials_supervisor)
    on_exit(fn -> if Process.alive?(occupied), do: Agent.stop(occupied) end)

    assert {:error,
            %{
              operation: :status,
              state: :authentication_unavailable,
              code: :authentication_lifecycle_conflict,
              safe_to_retry: true,
              completed_stages: [],
              findings: [finding]
            } = result} = Bootstrap.status(azure_env())

    assert finding.code == :authentication_lifecycle_conflict
    assert finding.stage == :bootstrap_connection
    assert finding.details.failure_class == "authentication_lifecycle_conflict"
    assert finding.details.diagnostic_id =~ ~r/^diag_[0-9a-f]{16}$/
    refute inspect(result) =~ "credential-canary"
  end

  test "an immediate linked-child death cannot exit the workflow caller" do
    assert {:error, :process_exited_during_start} =
             Connection.start_unlinked(fn ->
               child =
                 spawn_link(fn ->
                   receive do
                     :fail_start -> exit(:immediate_start_failure)
                   end
                 end)

               monitor = Process.monitor(child)
               send(child, :fail_start)
               assert_receive {:DOWN, ^monitor, :process, ^child, :immediate_start_failure}
               {:ok, child}
             end)

    refute Process.flag(:trap_exit, false)
  end

  defp azure_env do
    %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_DATABASE_HOST" => "127.0.0.1",
      "FAVN_DATABASE_PORT" => "5432",
      "FAVN_DATABASE_NAME" => "favn",
      "FAVN_DATABASE_MAINTENANCE_NAME" => "postgres",
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_BOOTSTRAP_USERNAME" => "favn_bootstrap",
      "FAVN_DATABASE_BOOTSTRAP_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "11111111-1111-1111-1111-111111111111",
      "FAVN_DATABASE_MIGRATOR_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_MIGRATOR_USERNAME" => "favn_migrator",
      "FAVN_DATABASE_MIGRATOR_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "22222222-2222-2222-2222-222222222222",
      "FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID" => "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_RUNTIME_USERNAME" => "favn_runtime",
      "FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "33333333-3333-3333-3333-333333333333",
      "FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID" => "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    }
  end
end

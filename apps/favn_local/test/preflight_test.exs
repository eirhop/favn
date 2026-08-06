defmodule FavnLocal.PreflightTest do
  use ExUnit.Case, async: true

  alias FavnLocal.Config
  alias FavnLocal.Preflight

  defmodule UpgradeRequiredRelease do
    def verify_runtime_schema do
      {:error,
       %{
         operation: :verify_schema,
         status: :error,
         code: :schema_not_ready,
         diagnostics: %{
           status: :upgrade_required,
           missing_migration_versions: [20_260_805_000_000]
         }
       }}
    end

    def verify_workspace(_workspace_id), do: {:ok, %{operation: :verify_workspace, status: :ok}}
  end

  defmodule IncompatibleRelease do
    def verify_runtime_schema do
      {:error,
       %{
         operation: :verify_schema,
         status: :error,
         code: :schema_not_ready,
         diagnostics: %{
           status: :incompatible,
           future_migration_versions: [20_270_101_000_000]
         }
       }}
    end

    def verify_workspace(_workspace_id), do: {:ok, %{operation: :verify_workspace, status: :ok}}
  end

  defmodule UnsupportedPostgresRelease do
    def verify_runtime_schema do
      {:error,
       %{
         operation: :verify_schema,
         status: :error,
         code: :schema_not_ready,
         diagnostics: %{
           status: :incompatible,
           engine: %{name: :postgresql, version: %{major: 17}}
         }
       }}
    end

    def verify_workspace(_workspace_id), do: {:ok, %{operation: :verify_workspace, status: :ok}}
  end

  defmodule ElevatedRuntimeRelease do
    def verify_runtime_schema do
      {:error,
       %{operation: :verify_schema, status: :error, code: :runtime_role_not_ready}}
    end

    def verify_workspace(_workspace_id), do: raise("workspace verification must not run")
  end

  test "reports the exact missing migration and one development upgrade command" do
    config = struct(Config, workspace_id: "local-dev")

    assert {:error,
            {:postgres_schema_not_ready, "missing migration 20260805000000",
             "mix favn.postgres.upgrade"}} =
             Preflight.run(config, release: UpgradeRequiredRelease)
  end

  test "does not recommend migrating an incompatible newer schema" do
    config = struct(Config, workspace_id: "local-dev")

    assert {:error,
            {:postgres_schema_not_ready,
             "database has migrations from a newer Favn version (20270101000000)", nil}} =
             Preflight.run(config, release: IncompatibleRelease)
  end

  test "does not recommend migrating an unsupported PostgreSQL major" do
    config = struct(Config, workspace_id: "local-dev")

    assert {:error,
            {:postgres_schema_not_ready,
             "PostgreSQL major 17 is unsupported; Favn requires PostgreSQL 18", nil}} =
             Preflight.run(config, release: UnsupportedPostgresRelease)
  end

  test "rejects an elevated runtime database connection before workspace verification" do
    config = struct(Config, workspace_id: "local-dev")

    assert {:error, :postgres_runtime_role_not_ready} =
             Preflight.run(config, release: ElevatedRuntimeRelease)
  end
end

defmodule Favn.PostgresStorageAdapterTest do
  use ExUnit.Case, async: true

  alias Favn.Storage.Adapter.Postgres

  defmodule FakePostgresRepoMissingTransact do
    def __adapter__, do: Ecto.Adapters.Postgres
    def rollback(_value), do: :ok
  end

  defmodule FakePostgresRepoMissingRollback do
    def __adapter__, do: Ecto.Adapters.Postgres
    def transact(fun), do: fun.()
  end

  defmodule NotARepo do
  end

  test "managed mode requires repo_config" do
    assert {:error, :postgres_repo_config_required} = Postgres.child_spec(repo_mode: :managed)
  end

  test "external mode requires repo module" do
    assert {:error, :postgres_external_repo_required} = Postgres.child_spec(repo_mode: :external)
  end

  test "external mode rejects non-postgres repo" do
    assert {:error, :postgres_external_repo_must_use_postgres} =
             Postgres.child_spec(repo_mode: :external, repo: Favn.Storage.SQLite.Repo)
  end

  test "external mode rejects non-repo modules" do
    assert {:error, :postgres_external_repo_invalid} =
             Postgres.child_spec(repo_mode: :external, repo: NotARepo)
  end

  test "external mode rejects repos missing transact" do
    assert {:error, :postgres_external_repo_missing_transact} =
             Postgres.child_spec(repo_mode: :external, repo: FakePostgresRepoMissingTransact)
  end

  test "external mode rejects repos missing rollback" do
    assert {:error, :postgres_external_repo_missing_rollback} =
             Postgres.child_spec(repo_mode: :external, repo: FakePostgresRepoMissingRollback)
  end

  test "external mode with postgres repo returns no managed child" do
    assert :none = Postgres.child_spec(repo_mode: :external, repo: Favn.Storage.Postgres.Repo)
  end

  test "external mode rejects auto migration mode" do
    assert {:error, :postgres_external_repo_auto_migration_unsupported} =
             Postgres.child_spec(
               repo_mode: :external,
               repo: Favn.Storage.Postgres.Repo,
               migration_mode: :auto
             )
  end

  test "invalid migration mode returns error" do
    assert {:error, {:invalid_migration_mode, :later}} =
             Postgres.child_spec(
               repo_mode: :managed,
               repo_config: [database: "favn"],
               migration_mode: :later
             )
  end
end

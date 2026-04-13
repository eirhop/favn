defmodule Favn.PostgresStorageAdapterTest do
  use ExUnit.Case, async: true

  alias Favn.Storage.Adapter.Postgres

  test "managed mode requires repo_config" do
    assert {:error, :postgres_repo_config_required} = Postgres.child_spec(repo_mode: :managed)
  end

  test "external mode requires repo module" do
    assert {:error, :postgres_external_repo_required} = Postgres.child_spec(repo_mode: :external)
  end

  test "external mode with repo returns no managed child" do
    assert :none = Postgres.child_spec(repo_mode: :external, repo: Favn.Storage.SQLite.Repo)
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

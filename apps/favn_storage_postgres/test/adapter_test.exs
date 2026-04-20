defmodule FavnStoragePostgres.AdapterTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.Adapter

  defmodule ExternalRepoStub do
    def __adapter__, do: Ecto.Adapters.Postgres
  end

  test "managed mode validates repo config" do
    assert {:error, {:invalid_repo_config, :hostname}} = Adapter.child_spec(repo_mode: :managed)

    assert {:error, {:invalid_repo_config, :hostname}} =
             Adapter.child_spec(repo_mode: :managed, repo_config: [])

    assert {:error, {:invalid_migration_mode, :bad}} =
             Adapter.child_spec(
               repo_mode: :managed,
               repo_config: [
                 hostname: "localhost",
                 database: "favn",
                 username: "postgres",
                 password: "postgres"
               ],
               migration_mode: :bad
             )
  end

  test "managed mode returns supervisor child spec" do
    assert {:ok, child_spec} =
             Adapter.child_spec(
               repo_mode: :managed,
               repo_config: [
                 hostname: "localhost",
                 database: "favn",
                 username: "postgres",
                 password: "postgres"
               ],
               migration_mode: :manual,
               supervisor_name: Module.concat([__MODULE__, "Supervisor"])
             )

    assert match?(%{id: _id, start: {_, _, _}}, child_spec)
  end

  test "external mode requires a postgres repo module and manual migration mode" do
    assert {:error, {:invalid_external_repo, nil}} =
             Adapter.child_spec(repo_mode: :external)

    assert :none =
             Adapter.child_spec(
               repo_mode: :external,
               repo: ExternalRepoStub,
               migration_mode: :manual
             )

    assert {:error, {:invalid_external_repo, :bad}} =
             Adapter.child_spec(repo_mode: :external, repo: :bad, migration_mode: :manual)

    assert {:error, {:invalid_external_migration_mode, :auto}} =
             Adapter.child_spec(
               repo_mode: :external,
               repo: ExternalRepoStub,
               migration_mode: :auto
             )
  end

  test "invalid repo mode is rejected" do
    assert {:error, {:invalid_repo_mode, :bogus}} = Adapter.child_spec(repo_mode: :bogus)
  end
end

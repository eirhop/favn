defmodule FavnStoragePostgres.DevelopmentUpgradeTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.DevelopmentConnection
  alias FavnStoragePostgres.DevelopmentUpgrade

  defmodule SuccessfulRelease do
    def migrate(env) do
      send(env[:test_pid], {:release_stage, :migrate, env["FAVN_DATABASE_URL"]})
      {:ok, %{operation: :migrate, status: :ok}}
    end

    def grant_runtime(env) do
      send(env[:test_pid], {:release_stage, :grant_runtime, env["FAVN_DATABASE_URL"]})
      {:ok, %{operation: :grant_runtime, status: :ok}}
    end

    def verify_runtime_schema(env) do
      send(env[:test_pid], {:release_stage, :verify_schema, env["FAVN_DATABASE_URL"]})
      {:ok, %{operation: :verify_schema, status: :ok}}
    end
  end

  defmodule GrantFailureRelease do
    def migrate(_env), do: {:ok, %{operation: :migrate, status: :ok}}

    def grant_runtime(_env) do
      {:error, %{operation: :grant_runtime, status: :error, code: :grant_failed}}
    end

    def verify_runtime_schema(_env),
      do: raise("verify_runtime_schema must not run after a grant failure")
  end

  defmodule UnsafeRuntimeRelease do
    def migrate(_env), do: {:ok, %{operation: :migrate, status: :ok}}
    def grant_runtime(_env), do: {:ok, %{operation: :grant_runtime, status: :ok}}

    def verify_runtime_schema(_env) do
      {:error, %{operation: :verify_schema, status: :error, code: :runtime_role_not_ready}}
    end
  end

  test "selects the migrator for writes and the runtime role for verification" do
    env = valid_env(%{test_pid: self()})

    assert {:ok, %{completed: [:migrate, :grant_runtime, :verify_schema]}} =
             DevelopmentUpgrade.run(
               env: env,
               release: SuccessfulRelease,
               progress_fun: fn stage -> send(self(), {:progress, stage}) end
             )

    assert_receive {:release_stage, :migrate, "ecto://migrator@localhost/favn"}
    assert_receive {:release_stage, :grant_runtime, "ecto://migrator@localhost/favn"}
    assert_receive {:release_stage, :verify_schema, "ecto://runtime@localhost/favn"}
    assert_receive {:progress, :migrate}
    assert_receive {:progress, :grant_runtime}
    assert_receive {:progress, :verify_schema}
  end

  test "reports partial completion and stops at the failed stage" do
    assert {:error,
            %{
              stage: :grant_runtime,
              completed: [:migrate],
              code: :grant_failed,
              outcome: :unknown
            }} =
             DevelopmentUpgrade.run(env: valid_env(), release: GrantFailureRelease)
  end

  test "rejects a runtime connection that resolves to an unsafe database role" do
    assert {:error,
            %{
              stage: :verify_schema,
              completed: [:migrate, :grant_runtime],
              code: :runtime_role_not_ready,
              outcome: :unknown
            }} =
             DevelopmentUpgrade.run(env: valid_env(), release: UnsafeRuntimeRelease)
  end

  test "requires both development database identities" do
    assert {:error,
            %{
              stage: :configuration,
              completed: [],
              code: :missing_environment,
              outcome: :not_started,
              variable: "FAVN_DATABASE_MIGRATOR_URL"
            }} =
             DevelopmentUpgrade.run(env: %{"FAVN_DATABASE_URL" => "ecto://runtime"})
  end

  test "rejects one URL serving as both runtime and migrator" do
    env = %{
      "FAVN_DATABASE_URL" => "ecto://same@localhost/favn",
      "FAVN_DATABASE_MIGRATOR_URL" => "ecto://same@localhost/favn"
    }

    assert {:error,
            %{
              stage: :configuration,
              completed: [],
              code: :database_roles_not_separated,
              outcome: :not_started
            }} =
             DevelopmentUpgrade.run(env: env)
  end

  test "development connection selection never replaces the caller environment" do
    env = valid_env()

    assert {:ok, migrator_env} = DevelopmentConnection.env_for(:migrator, env)
    assert migrator_env["FAVN_DATABASE_URL"] == "ecto://migrator@localhost/favn"
    assert env["FAVN_DATABASE_URL"] == "ecto://runtime@localhost/favn"
  end

  defp valid_env(overrides \\ %{}) do
    Map.merge(
      %{
        "FAVN_DATABASE_URL" => "ecto://runtime@localhost/favn",
        "FAVN_DATABASE_MIGRATOR_URL" => "ecto://migrator@localhost/favn"
      },
      overrides
    )
  end
end

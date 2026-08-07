defmodule FavnStoragePostgres.Bootstrap.ResultContractTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Bootstrap
  alias FavnStoragePostgres.Bootstrap.Result

  test "invalid configuration is bounded and telemetry and logs remain redacted" do
    handler = "bootstrap-result-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach_many(
      handler,
      [
        [:favn, :storage_postgres, :database_workflow, :start],
        [:favn, :storage_postgres, :database_workflow, :stop]
      ],
      fn event, measurements, metadata, _config ->
        send(parent, {event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    log =
      capture_log(fn ->
        assert {:error,
                %{
                  contract_version: 1,
                  operation: :bootstrap,
                  outcome: :failed,
                  state: :invalid_configuration,
                  code: :invalid_database_bootstrap_configuration,
                  duration_ms: duration,
                  safe_to_retry: true,
                  runtime_verified: false
                } = result} =
                 Bootstrap.bootstrap(%{
                   "FAVN_DATABASE_BOOTSTRAP_URL" =>
                     "ecto://favn_bootstrap:secret-canary@postgres.example/postgres"
                 })

        assert is_integer(duration) and duration >= 0
        refute inspect(result) =~ "secret-canary"
        refute inspect(result) =~ "postgres.example"
      end)

    refute log =~ "secret-canary"
    refute log =~ "postgres.example"

    assert_receive {[:favn, :storage_postgres, :database_workflow, :start],
                    %{system_time: system_time}, %{operation: :bootstrap}}

    assert is_integer(system_time)

    assert_receive {[:favn, :storage_postgres, :database_workflow, :stop],
                    %{duration_ms: duration_ms},
                    %{operation: :bootstrap, outcome: :failed, state: :invalid_configuration}}

    assert is_integer(duration_ms) and duration_ms >= 0
  end

  test "workspace failures with an uncertain commit acknowledgement are not retryable" do
    for kind <- [:timeout, :unavailable, :internal] do
      failure = Error.new(kind, "redacted database failure", retryable?: true)
      assert Bootstrap.classify_workspace_failure(failure) == :unknown_outcome

      {:error, result} =
        Result.error(
          :bootstrap,
          :unknown_outcome,
          Bootstrap.classify_workspace_failure(failure),
          :workspace
        )

      refute result.safe_to_retry
      assert Result.exit_code(result) == 76
    end

    conflict = Error.new(:conflict, "workspace already exists")
    assert Bootstrap.classify_workspace_failure(conflict) == :workspace_conflict
  end
end

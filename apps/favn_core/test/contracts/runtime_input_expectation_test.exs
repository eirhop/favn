Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule Favn.Contracts.RuntimeInputExpectationTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts.RuntimeInputExpectation
  alias Favn.Contracts.RuntimeInputResolutionRequest
  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Contracts.RunnerTask.PersistenceSchema
  alias Favn.RuntimeInput.Resolution
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  test "resolution tasks reference their package and retain only a bounded expectation" do
    {version, package} =
      Fixture.package_version("Elixir.InputAsset", "source", "Elixir.InputResolver")

    {:asset_attempt, work, _} = hd(Fixture.tasks(version))
    relation = Favn.RelationRef.new!(connection: :default, name: "source")

    work = %{
      work
      | execution_package: package,
        runtime_publication: nil,
        params: %{},
        metadata: %{},
        logical_target_id: Favn.TargetIdentity.for_asset(work.asset_ref),
        target_descriptor_hash: String.duplicate("a", 64),
        target_generation_id: "11111111-1111-4111-8111-111111111111",
        target_operation: :normal_materialization,
        active_relation: relation,
        write_relation: relation,
        rebuild_operation_id: "rebuild",
        rebuild_action_id: "action",
        rebuild_item_id: "item"
    }

    request = %RuntimeInputResolutionRequest{work: work}
    assert {:ok, encoded, _} = PersistenceCodec.encode_payload(:runtime_input_resolution, request)
    assert encoded["execution_package_hash"] == package.content_hash
    refute Jason.encode!(encoded) =~ "SELECT 1"

    assert {:ok, ^request} =
             PersistenceCodec.decode_payload(:runtime_input_resolution, encoded, version, [
               package
             ])

    assert {:error, _} =
             PersistenceCodec.decode_payload(:runtime_input_resolution, encoded, version, [])

    assert {:ok, resolution} =
             Resolution.new(
               resolver: InputResolver,
               params: %{secret: "private-sentinel"},
               metadata: %{secret: "private-metadata"},
               input_identity: "input-v1"
             )

    expectation = RuntimeInputExpectation.from_resolution(resolution)

    assert {:ok, encoded_result} =
             PersistenceCodec.encode_result(:runtime_input_resolution, :succeeded, expectation)

    refute Jason.encode!(encoded_result) =~ "private-"

    assert {:ok, ^expectation} =
             PersistenceCodec.decode_result(
               :runtime_input_resolution,
               :succeeded,
               encoded_result,
               version,
               [package]
             )

    assert :ok =
             PersistenceSchema.completion(
               :runtime_input_resolution,
               request,
               expectation,
               :succeeded
             )

    assert {:error, :runner_task_result_identity_mismatch} =
             PersistenceSchema.completion(
               :runtime_input_resolution,
               request,
               %{expectation | resolver: "Elixir.Other"},
               :succeeded
             )

    assert {:error, _} =
             RuntimeInputResolutionRequest.validate(%{
               request
               | work: %{work | metadata: %{runner_task_mode: :runtime_input_resolution}}
             })

    assert Favn.Contracts.RunnerTask.version() == 16
    assert PersistenceCodec.payload_version() == 2
  end

  test "expectation rejects malformed fingerprints and oversized identities" do
    expectation = %RuntimeInputExpectation{
      resolver: "Elixir.Resolver",
      input_identity: "input-v1",
      payload_fingerprint: String.duplicate("a", 64)
    }

    assert :ok = RuntimeInputExpectation.validate(expectation)

    assert {:error, _} =
             RuntimeInputExpectation.validate(%{expectation | payload_fingerprint: "wrong"})

    assert {:error, _} =
             RuntimeInputExpectation.validate(%{
               expectation
               | input_identity: String.duplicate("x", 10_000)
             })
  end

  test "resolution results cannot authorize automatic retry or unknown-write recovery" do
    alias Favn.Contracts.RunnerTask
    alias Favn.Contracts.RunnerError
    error = RunnerError.new(reason: :lost, outcome: :safe_failure, retryable?: false)

    assert :ok =
             RunnerTask.validate_terminal_retry(
               :runtime_input_resolution,
               :failed,
               :terminal,
               error
             )

    assert {:error, _} =
             RunnerTask.validate_terminal_retry(
               :runtime_input_resolution,
               :failed,
               :safe_to_retry,
               %{error | retryable?: true}
             )

    assert {:error, _} =
             RunnerTask.validate_terminal_retry(
               :runtime_input_resolution,
               :unknown,
               :unknown_do_not_retry,
               %{error | outcome: :unknown}
             )

    assert :ok =
             RunnerTask.validate_terminal_retry(
               :asset_attempt,
               :unknown,
               :unknown_do_not_retry,
               %{error | outcome: :unknown}
             )
  end
end

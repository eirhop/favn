defmodule Favn.Contracts.RunnerTaskTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerTask.NoWork
  alias Favn.Contracts.RunnerTask.Registration
  alias Favn.Contracts.RunnerTask.Result
  alias Favn.Manifest.Version

  @release FavnTestSupport.runner_release_id()

  test "registration round trips without creating a pool atom" do
    pool = "duckdb_#{System.unique_integer([:positive])}"
    assert_raise ArgumentError, fn -> String.to_existing_atom(pool) end

    message = %Registration{
      runner_instance_id: "runner-1",
      boot_id: "boot-1",
      beam_node: "runner-1@private",
      runner_pool: pool,
      required_runner_release_id: @release,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection]
    }

    assert :ok = Registration.validate(message)
    assert {:ok, encoded} = Registration.encode(message)
    assert {:ok, ^message} = Registration.decode(encoded)
    assert_raise ArgumentError, fn -> String.to_existing_atom(pool) end
  end

  test "assignment requires a typed operation payload" do
    assignment =
      assignment(
        :generation_capabilities,
        %GenerationCapabilitiesRequest{
          manifest: %Version{},
          asset_ref: {MyApp.Asset, :asset}
        }
      )

    assert :ok = Assignment.validate(assignment)

    assert {:error, {:invalid_runner_task_payload, :generation_capabilities, %{}}} =
             assignment
             |> Map.put(:payload, %{})
             |> Assignment.validate()
  end

  test "generation marker read policy survives durable payload round trips" do
    request = %GenerationMarkerReadRequest{
      manifest: %Version{},
      asset_ref: {MyApp.Asset, :asset},
      require_relation_instance?: false
    }

    assert {:ok, encoded, _hash} =
             Favn.Contracts.RunnerTask.PersistenceCodec.encode_payload(
               :generation_marker_read,
               request
             )

    assert {:ok, ^request} =
             Favn.Contracts.RunnerTask.PersistenceCodec.decode_payload(
               :generation_marker_read,
               encoded,
               sized_version()
             )

    assert {:error, {:invalid_generation_marker_read_request, _invalid}} =
             request
             |> Map.put(:require_relation_instance?, nil)
             |> GenerationMarkerReadRequest.validate()
  end

  test "control-plane wait instructions are bounded" do
    no_work = %NoWork{
      command_id: "claim-empty",
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      action: :wait,
      wait_ms: 15_000
    }

    assert :ok = NoWork.validate(no_work)

    assert {:error, {:invalid_runner_task_wait_ms, -1}} =
             no_work |> Map.put(:wait_ms, -1) |> NoWork.validate()

    assert {:error, {:invalid_runner_task_wait_ms, 3_600_001}} =
             no_work |> Map.put(:wait_ms, 3_600_001) |> NoWork.validate()
  end

  test "successful results require the result type for their task kind" do
    result = %Result{
      workspace_id: "workspace-1",
      task_id: "rt_result",
      task_kind: :generation_capabilities,
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      outcome: :succeeded,
      retry_class: :terminal,
      result: %GenerationCapabilitiesResult{capabilities: %{atomic_swap: :supported}},
      finished_at: DateTime.utc_now()
    }

    assert :ok = Result.validate(result)

    assert {:error, {:invalid_runner_task_result, :generation_capabilities, :succeeded, %{}}} =
             result
             |> Map.put(:result, %{})
             |> Result.validate()
  end

  test "redaction removes task payloads and errors" do
    result = %Result{
      workspace_id: "workspace-1",
      task_id: "rt_result",
      task_kind: :asset_attempt,
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      outcome: :failed,
      retry_class: :unknown_do_not_retry,
      result: nil,
      error: RunnerError.normalize(%{secret: "hidden"}),
      finished_at: DateTime.utc_now()
    }

    assert %{error: "[REDACTED]", result: "[REDACTED]"} = Result.redact(result)
  end

  test "terminal retry classification requires matching outcome evidence" do
    base = %Result{
      workspace_id: "workspace-1",
      task_id: "rt_result",
      task_kind: :asset_attempt,
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      outcome: :failed,
      retry_class: :safe_to_retry,
      result: nil,
      error: RunnerError.new(outcome: :unknown, retryable?: true),
      finished_at: DateTime.utc_now()
    }

    assert {:error, {:invalid_runner_task_retry_classification, _, _, _, _}} =
             Result.validate(base)

    safe =
      %{base | error: RunnerError.new(outcome: :safe_failure, retryable?: true)}

    assert :ok = Result.validate(safe)
  end

  test "runtime input resolution messages enforce mutually exclusive typed outcomes" do
    {:ok, resolution} =
      Favn.RuntimeInput.Resolution.new(%{
        resolver: __MODULE__,
        params: %{region: "eu"},
        input_identity: "settings-v1",
        metadata: %{},
        sensitive_params: []
      })

    base = %Favn.Contracts.RunnerTask.RuntimeInputsResolved{
      workspace_id: "workspace-1",
      task_id: "rt_inputs",
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      resolution_id: "resolution-1",
      issued_at: ~U[2026-07-28 00:00:00Z],
      status: :resolved,
      runtime_inputs: resolution
    }

    assert :ok = Favn.Contracts.RunnerTask.RuntimeInputsResolved.validate(base)
    assert {:ok, encoded} = Favn.Contracts.RunnerTask.RuntimeInputsResolved.encode(base)
    assert {:ok, ^base} = Favn.Contracts.RunnerTask.RuntimeInputsResolved.decode(encoded)

    %RunnerError{} = unredacted_error = RunnerError.new(outcome: :unknown)
    unredacted_error = %RunnerError{unredacted_error | redacted?: false}

    invalid_messages = [
      %{base | runtime_inputs: nil},
      %{base | error: RunnerError.new(outcome: :unknown)},
      %{base | runtime_inputs: %{region: "eu"}},
      %{
        base
        | runtime_inputs: %{resolution | payload_fingerprint: String.duplicate("0", 64)}
      },
      %{base | status: :failed, runtime_inputs: resolution, error: nil},
      %{
        base
        | status: :failed,
          runtime_inputs: nil,
          error: unredacted_error
      }
    ]

    Enum.each(invalid_messages, fn message ->
      assert {:error, _reason} =
               Favn.Contracts.RunnerTask.RuntimeInputsResolved.validate(message)
    end)

    failed = %{
      base
      | status: :failed,
        runtime_inputs: nil,
        error: RunnerError.new(outcome: :unknown)
    }

    assert :ok = Favn.Contracts.RunnerTask.RuntimeInputsResolved.validate(failed)
    assert {:ok, failed_encoded} = Favn.Contracts.RunnerTask.RuntimeInputsResolved.encode(failed)

    assert {:ok, ^failed} =
             Favn.Contracts.RunnerTask.RuntimeInputsResolved.decode(failed_encoded)
  end

  test "failed runtime input acknowledgements do not invent a payload fingerprint" do
    acknowledgement = %Favn.Contracts.RunnerTask.RuntimeInputsAck{
      workspace_id: "workspace-1",
      task_id: "rt_inputs",
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      resolution_id: "resolution-failed",
      payload_fingerprint: nil,
      status: :persisted
    }

    assert :ok = Favn.Contracts.RunnerTask.RuntimeInputsAck.validate(acknowledgement)
  end

  test "every protocol 13 message validates and round trips" do
    now = DateTime.utc_now()

    {:ok, runtime_inputs} =
      Favn.RuntimeInput.Resolution.new(%{
        resolver: __MODULE__,
        params: %{"secret" => "wire-only"},
        input_identity: "runtime-inputs",
        metadata: %{},
        sensitive_params: ["secret"]
      })

    messages = [
      %Favn.Contracts.RunnerTask.Registration{
        runner_instance_id: "runner-1",
        boot_id: "boot-1",
        runner_session_generation: 1,
        beam_node: "runner-1@private",
        runner_pool: "duckdb",
        required_runner_release_id: @release,
        lifecycle_mode: :elastic,
        supported_task_kinds: [:relation_inspection],
        capabilities: ["relation_inspection"]
      },
      %Favn.Contracts.RunnerTask.RegistrationAck{
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        status: :accepted
      },
      %Favn.Contracts.RunnerTask.ClaimRequest{
        command_id: "claim-1",
        issued_at: now,
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        runner_pool: "duckdb",
        required_runner_release_id: @release,
        supported_task_kinds: [:relation_inspection],
        capabilities: ["relation_inspection"]
      },
      assignment(
        :generation_capabilities,
        %GenerationCapabilitiesRequest{
          manifest: %Version{},
          asset_ref: {MyApp.Asset, :asset}
        }
      ),
      %Favn.Contracts.RunnerTask.NoWork{
        command_id: "claim-empty-1",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        action: :wait,
        wait_ms: 15_000
      },
      %Favn.Contracts.RunnerTask.Wake{
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        runner_pool: "duckdb",
        required_runner_release_id: @release
      },
      %Favn.Contracts.RunnerTask.Started{
        workspace_id: "workspace-1",
        task_id: "rt_started",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        issued_at: now,
        occurred_at: now
      },
      %Favn.Contracts.RunnerTask.LeaseRenewal{
        workspace_id: "workspace-1",
        task_id: "rt_renewal",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        lease_expires_at: now
      },
      %Favn.Contracts.RunnerTask.RuntimeInputsResolved{
        workspace_id: "workspace-1",
        task_id: "rt_inputs",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        resolution_id: "resolution-1",
        issued_at: now,
        status: :resolved,
        runtime_inputs: runtime_inputs
      },
      %Favn.Contracts.RunnerTask.RuntimeInputsAck{
        workspace_id: "workspace-1",
        task_id: "rt_inputs",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        resolution_id: "resolution-1",
        payload_fingerprint: :crypto.hash(:sha256, "runtime-inputs"),
        status: :persisted
      },
      %Favn.Contracts.RunnerTask.LogBatch{
        workspace_id: "workspace-1",
        task_id: "rt_logs",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        batch_id: "batch-1",
        issued_at: now,
        sequence: 0,
        entries: [%{"message" => "hello"}]
      },
      %Favn.Contracts.RunnerTask.LogAck{
        workspace_id: "workspace-1",
        task_id: "rt_logs",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        batch_id: "batch-1",
        sequence: 0
      },
      %Favn.Contracts.RunnerTask.Result{
        workspace_id: "workspace-1",
        task_id: "rt_result",
        task_kind: :asset_attempt,
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        outcome: :failed,
        retry_class: :unknown_do_not_retry,
        error: RunnerError.normalize(:test_failure),
        finished_at: now
      },
      %Favn.Contracts.RunnerTask.ResultAck{
        workspace_id: "workspace-1",
        task_id: "rt_result",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        status: :persisted
      },
      %Favn.Contracts.RunnerTask.Cancellation{
        workspace_id: "workspace-1",
        task_id: "rt_cancel",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        command_id: "cancel-1",
        reason: :operator_request,
        requested_at: now
      },
      %Favn.Contracts.RunnerTask.CancellationAck{
        workspace_id: "workspace-1",
        task_id: "rt_cancel",
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        assignment_generation: 1,
        command_id: "cancel-1",
        status: :observed,
        issued_at: now,
        acknowledged_at: now
      },
      %Favn.Contracts.RunnerTask.Shutdown{
        runner_instance_id: "runner-1",
        runner_session_generation: 1,
        action: :stop,
        reason: :idle
      }
    ]

    Enum.each(messages, fn message ->
      module = message.__struct__
      assert :ok = module.validate(message)
      assert {:ok, encoded} = module.encode(message)
      assert {:ok, ^message} = module.decode(encoded)
    end)
  end

  test "task-scoped messages reject zero assignment fences" do
    message = %Favn.Contracts.RunnerTask.Started{
      workspace_id: "workspace-1",
      task_id: "rt_started",
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 0,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    }

    assert {:error, {:invalid_runner_task_assignment_fence, 1, 0}} =
             Favn.Contracts.RunnerTask.Started.validate(message)
  end

  test "session-scoped messages and accepted registration reject zero session fences" do
    messages = [
      %Favn.Contracts.RunnerTask.RegistrationAck{
        runner_instance_id: "runner-1",
        runner_session_generation: 0,
        status: :accepted
      },
      %Favn.Contracts.RunnerTask.ClaimRequest{
        command_id: "claim-zero",
        issued_at: ~U[2026-07-28 00:00:00Z],
        runner_instance_id: "runner-1",
        runner_session_generation: 0,
        runner_pool: "duckdb",
        required_runner_release_id: @release,
        supported_task_kinds: [:relation_inspection],
        capabilities: ["relation_inspection"]
      },
      %Favn.Contracts.RunnerTask.NoWork{
        command_id: "claim-zero",
        runner_instance_id: "runner-1",
        runner_session_generation: 0
      },
      %Favn.Contracts.RunnerTask.Wake{
        runner_instance_id: "runner-1",
        runner_session_generation: 0,
        runner_pool: "duckdb",
        required_runner_release_id: @release
      },
      %Favn.Contracts.RunnerTask.Shutdown{
        runner_instance_id: "runner-1",
        runner_session_generation: 0
      }
    ]

    Enum.each(messages, fn message ->
      assert {:error, {:invalid_runner_task_session_fence, 0}} =
               message.__struct__.validate(message)
    end)
  end

  test "wire codec rejects compressed external terms before decoding" do
    message = %Favn.Contracts.RunnerTask.NoWork{
      command_id: "compressed",
      runner_instance_id: "runner-1",
      runner_session_generation: 1
    }

    compressed_payload =
      message
      |> Map.from_struct()
      |> :erlang.term_to_binary(compressed: 9)
      |> Base.encode64()

    assert {:error, :invalid_runner_task_payload} =
             Favn.Contracts.RunnerTask.NoWork.decode(%{
               "type" => "no_work",
               "version" => 13,
               "payload" => compressed_payload
             })
  end

  test "classify_failure follows the error envelope" do
    assert {:failed, :safe_to_retry} =
             classify(:asset_attempt, outcome: :safe_failure, retryable?: true)

    assert {:failed, :terminal} =
             classify(:asset_attempt, outcome: :safe_failure, retryable?: false)

    assert {:unknown, :unknown_do_not_retry} = classify(:asset_attempt, outcome: :unknown)
    assert {:unknown, :unknown_do_not_retry} = classify(:relation_inspection, outcome: :unknown)
    assert {:unknown, :reconcile_before_retry} = classify(:generation_activate, outcome: :unknown)
    assert {:cancelled, :terminal} = classify(:asset_attempt, outcome: :cancelled)
  end

  test "classify_failure always yields a pair validate_terminal_retry accepts" do
    errors =
      for outcome <- [:safe_failure, :unknown, :cancelled],
          retryable? <- [true, false] do
        RunnerError.new(outcome: outcome, retryable?: retryable?)
      end

    for kind <- Favn.Contracts.RunnerTask.task_kinds(), error <- errors do
      {outcome, retry_class} = Favn.Contracts.RunnerTask.classify_failure(kind, error)

      assert :ok ==
               Favn.Contracts.RunnerTask.validate_terminal_retry(
                 kind,
                 outcome,
                 retry_class,
                 error
               ),
             "kind #{inspect(kind)} with error #{inspect({error.outcome, error.retryable?})} " <>
               "classified as unacceptable #{inspect({outcome, retry_class})}"
    end
  end

  test "asset payload semantic and encoded bounds reject excess bytes" do
    alias Favn.Contracts.RunnerTask.Limits
    alias Favn.Contracts.RunnerTask.PersistenceCodec

    limit = Limits.payload_bytes(:asset_attempt)
    work = sized_work(limit)
    assert byte_size(:erlang.term_to_binary(work, [:deterministic])) == limit
    assert {:ok, envelope, hash} = PersistenceCodec.encode_payload(:asset_attempt, work)
    assert byte_size(Jason.encode!(envelope)) <= 4 * limit

    assert {:ok, ^work} =
             PersistenceCodec.decode_payload(:asset_attempt, envelope, sized_version())

    assert {:ok, ^hash} = PersistenceCodec.payload_hash(envelope)

    oversized = sized_work(limit + 1)

    assert {:error, {:runner_task_payload_too_large, actual, ^limit}} =
             PersistenceCodec.encode_payload(:asset_attempt, oversized)

    assert actual == limit + 1

    # The extra byte can share the same padded base64 length: check decoded size too.
    forged =
      Map.put(envelope, "payload", oversized |> :erlang.term_to_binary() |> Base.encode64())

    assert {:error, :invalid_runner_task_persistence_envelope} =
             PersistenceCodec.decode_payload(:asset_attempt, forged)

    assert {:error, :invalid_runner_task_persistence_envelope} =
             PersistenceCodec.decode_payload(
               :asset_attempt,
               Map.put(
                 envelope,
                 "payload",
                 String.duplicate("A", Limits.encoded_bytes(limit) + 1)
               )
             )
  end

  test "full-sized work fits assignment envelope and wire round trip with fences intact" do
    alias Favn.Contracts.RunnerTask.Limits
    work = sized_work(Limits.payload_bytes(:asset_attempt))
    message = assignment(:asset_attempt, work)
    assert :ok = Assignment.validate(message)
    assert {:ok, encoded} = Assignment.encode(message)
    assert byte_size(encoded["payload"]) > 2 * 1_048_576
    assert {:ok, ^message} = Assignment.decode(encoded)
    assert byte_size(:erlang.term_to_binary(message)) <= Assignment.payload_size_limit()

    assert {:error, {:runner_task_payload_too_large, _, 8_388_608}} =
             Assignment.validate(%{message | payload: sized_work(8_388_609)})

    assert {:error, :runner_task_asset_binding_mismatch} =
             Assignment.validate(%{message | runner_pool: "other"})

    assert {:error, _} = Assignment.validate(%{message | runner_session_generation: 0})
    assert {:error, _} = Assignment.validate(%{message | assignment_generation: 0})
  end

  test "assignment raw envelope and encoded bounds reject one byte over" do
    alias Favn.Contracts.RunnerTask.Contract
    alias Favn.Contracts.RunnerTask.Limits

    limit = Assignment.payload_size_limit()
    message = assignment(:asset_attempt, sized_work(1_000))
    overhead = byte_size(:erlang.term_to_binary(message)) - 1_000
    at_limit = %{message | payload: sized_work(limit - overhead)}
    assert byte_size(:erlang.term_to_binary(at_limit)) == limit
    assert :ok = Contract.validate(at_limit, [], [], limit, true)
    above = %{message | payload: sized_work(limit - overhead + 1)}
    assert {:error, {:runner_task_payload_too_large, actual, ^limit}} = Assignment.validate(above)
    assert actual == limit + 1

    assert Limits.wire_bytes(Assignment) == 11_272_192

    for raw <- [limit - 1, limit, limit + 1] do
      assert byte_size(Base.encode64(:binary.copy(<<0>>, raw))) == Limits.encoded_bytes(raw)
    end

    assert {:ok, encoded} = Assignment.encode(message)

    assert {:error, :invalid_runner_task_payload} =
             Assignment.decode(
               Map.put(
                 encoded,
                 "payload",
                 String.duplicate("A", Limits.wire_bytes(Assignment) + 1)
               )
             )

    # Raw map fields must be bounded before decoding, even if unknown fields
    # would be discarded by struct construction.
    oversized_fields =
      message |> Map.from_struct() |> Map.put(:padding, :binary.copy(<<0>>, limit))

    assert {:error, :invalid_runner_task_payload} =
             Assignment.decode(
               Map.put(
                 encoded,
                 "payload",
                 oversized_fields |> :erlang.term_to_binary() |> Base.encode64()
               )
             )

    assert {:error, {:runner_task_encoded_payload_too_large, _, 11_272_192}} =
             Favn.Contracts.RunnerTask.Codec.encode(
               %{message | payload: sized_work(limit + 10)},
               "assignment",
               fn _ -> :ok end
             )

    compressed =
      message |> Map.from_struct() |> :erlang.term_to_binary([:compressed]) |> Base.encode64()

    assert {:error, :invalid_runner_task_payload} =
             Assignment.decode(Map.put(encoded, "payload", compressed))
  end

  test "non-asset payloads, results, and log messages retain their bounds" do
    alias Favn.Contracts.RunnerTask.Limits
    alias Favn.Contracts.RunnerTask.PersistenceCodec

    operation = %Favn.Contracts.RelationInspectionRequest{
      manifest_version_id: "mv_sized",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: @release,
      relation: %Favn.RelationRef{connection: :default, name: :binary.copy(<<0>>, 1_048_576)}
    }

    assert {:error, {:runner_task_payload_too_large, _, 1_048_576}} =
             PersistenceCodec.encode_payload(:relation_inspection, operation)

    assert {:error, {:runner_task_payload_too_large, _, 1_048_576}} =
             Assignment.validate(assignment(:relation_inspection, operation))

    result = %Favn.Contracts.RunnerResult{
      manifest_version_id: "mv_sized",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: @release,
      status: :ok,
      metadata: %{"padding" => :binary.copy(<<0>>, 1_048_576)}
    }

    assert {:error, {:runner_task_payload_too_large, _, 1_048_576}} =
             PersistenceCodec.encode_result(:asset_attempt, :succeeded, result)

    assert Result.payload_size_limit() == 1_048_576
    assert Favn.Contracts.RunnerTask.LogBatch.payload_size_limit() == 262_144
    assert Limits.wire_bytes(Result) == 2_097_152
  end

  defp sized_version,
    do: %Version{
      manifest: %Favn.Manifest{
        assets: [
          %Favn.Manifest.Asset{
            ref: {MyApp.Asset, :asset},
            module: MyApp.Asset,
            name: :asset,
            runner_pool: :duckdb
          }
        ]
      }
    }

  defp sized_work(bytes) do
    work = %Favn.Contracts.RunnerWork{
      manifest_version_id: "mv_sized",
      manifest_content_hash: String.duplicate("a", 64),
      asset_ref: {MyApp.Asset, :asset},
      runner_pool: :duckdb,
      required_runner_release_id: @release,
      metadata: %{"padding" => ""}
    }

    overhead = byte_size(:erlang.term_to_binary(work, [:deterministic]))
    %{work | metadata: %{"padding" => :binary.copy(<<0>>, bytes - overhead)}}
  end

  defp classify(kind, fields) do
    Favn.Contracts.RunnerTask.classify_failure(kind, RunnerError.new(fields))
  end

  defp assignment(kind, payload) do
    %Assignment{
      command_id: "claim-assignment",
      workspace_id: "workspace-1",
      task_id: "rt_assignment",
      task_kind: kind,
      runner_instance_id: "runner-1",
      runner_session_generation: 1,
      assignment_generation: 1,
      runner_pool: "duckdb",
      required_runner_release_id: @release,
      assigned_at: DateTime.utc_now(),
      lease_expires_at: DateTime.utc_now(),
      retry_class: :safe_to_retry,
      payload: payload
    }
  end
end

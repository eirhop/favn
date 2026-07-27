defmodule Favn.Contracts.RunnerTaskTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerTask.Assignment
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

  test "private orchestration context round trips without entering the runner payload" do
    context = %{
      kind: :pipeline,
      materialization_claim: %{claim_key: "claim-1", fencing_token: 3},
      decision: %{status: :stale}
    }

    assert {:ok, envelope} =
             Favn.Contracts.RunnerTask.PersistenceCodec.encode_orchestration_context(context)

    assert {:ok, ^context} =
             Favn.Contracts.RunnerTask.PersistenceCodec.decode_orchestration_context(envelope)
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
      lease_expires_at: DateTime.utc_now(),
      retry_class: :safe_to_retry,
      payload: payload
    }
  end
end

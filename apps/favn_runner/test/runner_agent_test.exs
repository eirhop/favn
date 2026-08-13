defmodule FavnRunner.RunnerAgentTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerResult
  alias FavnRunner.RunnerAgent
  alias FavnRunner.TaskExecutor

  defmodule FakeControlPlane do
    use GenServer

    def start_link(owner) when is_pid(owner), do: start_link({owner, 60_000})
    def start_link({owner, wait_ms}), do: GenServer.start_link(__MODULE__, {owner, wait_ms})

    @impl true
    def init({owner, wait_ms}), do: {:ok, %{owner: owner, wait_ms: wait_ms}}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:registered, registration, agent})

      {:reply,
       {:ok,
        %RunnerTask.RegistrationAck{
          runner_instance_id: registration.runner_instance_id,
          runner_session_generation: max(registration.runner_session_generation, 1),
          status: :accepted
        }}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{} = request}, _from, state) do
      send(state.owner, {:claimed, request})

      {:reply,
       {:ok,
        %RunnerTask.NoWork{
          command_id: request.command_id,
          runner_instance_id: request.runner_instance_id,
          runner_session_generation: request.runner_session_generation,
          action: :wait,
          wait_ms: state.wait_ms
        }}, state}
    end

    def handle_call({:request, %RunnerTask.CancellationAck{} = acknowledgement}, _from, state) do
      send(state.owner, {:cancellation_ack, acknowledgement})
      {:reply, {:ok, acknowledgement}, state}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:lease_renewal, renewal})
      {:reply, {:error, :control_plane_unavailable}, state}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_delivery, result})
      {:reply, {:error, %{kind: :fenced}}, state}
    end
  end

  defmodule RejectedControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call(:gateway, _from, owner), do: {:reply, {:ok, self()}, owner}

    def handle_call({:register, registration, agent}, _from, owner) do
      send(owner, {:rejected_registration, registration, agent})

      {:reply,
       {:ok,
        %RunnerTask.RegistrationAck{
          runner_instance_id: registration.runner_instance_id,
          runner_session_generation: registration.runner_session_generation,
          status: :rejected,
          reason: :runner_lifecycle_mode_mismatch
        }}, owner}
    end
  end

  defmodule FakeExecutor do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:cancel, reason}, _from, owner) do
      send(owner, {:executor_cancelled, reason})
      {:reply, :ok, owner}
    end
  end

  defmodule FinishedExecutor do
    use GenServer

    def start_link(_owner), do: GenServer.start_link(__MODULE__, nil)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:cancel, _reason}, _from, state),
      do: {:reply, {:error, :already_finished}, state}
  end

  defmodule RuntimeInputReplayControlPlane do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state),
      do: {:ok, Map.merge(state, %{agent: nil, assignment: nil, runtime_inputs_seen: 0})}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      assignment =
        state.assignment ||
          %RunnerTask.Assignment{
            command_id: "runtime-input-claim",
            workspace_id: "workspace-runtime-input",
            task_id: "rt_runtime_input_replay",
            task_kind: :asset_attempt,
            runner_instance_id: registration.runner_instance_id,
            runner_session_generation: 1,
            assignment_generation: 1,
            runner_pool: "duckdb",
            required_runner_release_id: FavnTestSupport.runner_release_id(),
            assigned_at: DateTime.utc_now(),
            lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
            retry_class: :unknown_do_not_retry,
            payload: state.work
          }

      send(state.owner, {:runtime_input_registration, registration, agent})

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, %{state | assignment: assignment, agent: agent}}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: nil} = state
        ) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call(
          {:request, %RunnerTask.RuntimeInputsResolved{} = message},
          _from,
          %{runtime_inputs_seen: 0} = state
        ) do
      send(state.owner, {:runtime_inputs_committed_before_ack, message})
      {:reply, {:error, :control_plane_unavailable}, %{state | runtime_inputs_seen: 1}}
    end

    def handle_call(
          {:request, %RunnerTask.RuntimeInputsResolved{} = message},
          _from,
          state
        ) do
      send(state.owner, {:runtime_inputs_replayed, message})

      ack = %RunnerTask.RuntimeInputsAck{
        workspace_id: message.workspace_id,
        task_id: message.task_id,
        runner_instance_id: message.runner_instance_id,
        runner_session_generation: message.runner_session_generation,
        assignment_generation: message.assignment_generation,
        resolution_id: message.resolution_id,
        payload_fingerprint: message.runtime_inputs.payload_fingerprint,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | runtime_inputs_seen: state.runtime_inputs_seen + 1}}
    end

    def handle_call({:request, %RunnerTask.Started{}}, _from, state),
      do: {:reply, {:error, :stop_after_runtime_input_replay}, state}

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule PreparationFailureControlPlane do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state), do: {:ok, Map.merge(state, %{assignment: nil, result_replies: []})}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:registered, registration, agent})

      assignment = %RunnerTask.Assignment{
        command_id: "prep-failure-claim",
        workspace_id: "workspace-prep-failure",
        task_id: "rt_" <> state.work.run_id,
        task_kind: :asset_attempt,
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        assignment_generation: 1,
        runner_pool: "duckdb",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        assigned_at: DateTime.utc_now(),
        lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
        retry_class: :unknown_do_not_retry,
        payload: state.work
      }

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack},
       %{state | assignment: state.assignment || assignment}
       |> Map.put(:result_replies, Map.get(state, :reject_results, []))}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: nil} = state
        ) do
      send(state.owner, {:claimed_next, request})

      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call(
          {:request, %RunnerTask.RuntimeInputsResolved{} = message},
          _from,
          %{reject_runtime_inputs: reason} = state
        )
        when not is_nil(reason) do
      send(state.owner, {:runtime_inputs_rejected, message, reason})
      {:reply, {:error, reason}, state}
    end

    def handle_call({:request, %RunnerTask.RuntimeInputsResolved{} = message}, _from, state) do
      send(state.owner, {:runtime_inputs_reported, message})

      ack = %RunnerTask.RuntimeInputsAck{
        workspace_id: message.workspace_id,
        task_id: message.task_id,
        runner_instance_id: message.runner_instance_id,
        runner_session_generation: message.runner_session_generation,
        assignment_generation: message.assignment_generation,
        resolution_id: message.resolution_id,
        payload_fingerprint: message.runtime_inputs && message.runtime_inputs.payload_fingerprint,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state),
      do: {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}

    def handle_call(
          {:request, %RunnerTask.Result{} = result},
          _from,
          %{result_replies: [reply | rest]} = state
        ) do
      send(state.owner, {:result_rejected, result, reply})
      {:reply, {:error, reply}, %{state | result_replies: rest}}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_delivered, result, RunnerTask.Result.validate(result)})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | assignment: nil}}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule StaleResumeControlPlane do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state), do: {:ok, Map.merge(state, %{assignment: nil})}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call(
          {:register, %RunnerTask.Registration{active_assignment: nil} = registration, agent},
          _from,
          state
        ) do
      send(state.owner, {:fresh_registration, registration, agent})

      assignment =
        if state.assignment == :consumed do
          nil
        else
          %RunnerTask.Assignment{
            command_id: "stale-resume-claim",
            workspace_id: "workspace-stale-resume",
            task_id: "rt_" <> state.work.run_id,
            task_kind: :asset_attempt,
            runner_instance_id: registration.runner_instance_id,
            runner_session_generation: 1,
            assignment_generation: 1,
            runner_pool: "duckdb",
            required_runner_release_id: FavnTestSupport.runner_release_id(),
            assigned_at: DateTime.utc_now(),
            lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
            retry_class: :unknown_do_not_retry,
            payload: state.work
          }
        end

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, %{state | assignment: assignment}}
    end

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:resume_rejected, registration, agent})

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: registration.runner_session_generation,
        status: :rejected,
        reason: :stale_runner_task_resume
      }

      {:reply, {:ok, ack}, %{state | assignment: :consumed}}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: assignment} = state
        )
        when assignment in [nil, :consumed] do
      send(state.owner, {:claimed_after_drop, request})

      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call({:request, %RunnerTask.RuntimeInputsResolved{} = message}, _from, state) do
      ack = %RunnerTask.RuntimeInputsAck{
        workspace_id: message.workspace_id,
        task_id: message.task_id,
        runner_instance_id: message.runner_instance_id,
        runner_session_generation: message.runner_session_generation,
        assignment_generation: message.assignment_generation,
        resolution_id: message.resolution_id,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state),
      do: {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_attempted, result})
      {:reply, {:error, :control_plane_unavailable}, state}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule LeaseLossControlPlane do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state), do: {:ok, Map.merge(state, %{assignment: nil, served?: false})}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:registered, registration, agent})

      assignment =
        if state.served? do
          nil
        else
          %RunnerTask.Assignment{
            command_id: "lease-loss-claim",
            workspace_id: "workspace-lease-loss",
            task_id: "rt_" <> state.work.run_id,
            task_kind: :asset_attempt,
            runner_instance_id: registration.runner_instance_id,
            runner_session_generation: 1,
            assignment_generation: 1,
            runner_pool: "duckdb",
            required_runner_release_id: FavnTestSupport.runner_release_id(),
            assigned_at: DateTime.utc_now(),
            lease_expires_at: DateTime.add(DateTime.utc_now(), 300, :millisecond),
            retry_class: :unknown_do_not_retry,
            payload: state.work
          }
        end

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: max(registration.runner_session_generation, 1),
        status: :accepted
      }

      {:reply, {:ok, ack}, %{state | assignment: state.assignment || assignment}}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: nil} = state
        ) do
      send(state.owner, {:claimed_next, request})

      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, %{state | served?: true}}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:renewal_failed, renewal})
      {:reply, {:error, :control_plane_unavailable}, state}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_delivered, result, RunnerTask.Result.validate(result)})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | assignment: nil}}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule RestartingControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)
    def fail_next(server, kind), do: GenServer.call(server, {:fail_next, kind})

    @impl true
    def init(owner),
      do: {:ok, %{owner: owner, registration_required?: false, fail_next: nil}}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:fail_next, kind}, _from, state),
      do: {:reply, :ok, %{state | fail_next: kind}}

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:restart_registration, registration, agent})

      reply =
        {:ok,
         %RunnerTask.RegistrationAck{
           runner_instance_id: registration.runner_instance_id,
           runner_session_generation: max(registration.runner_session_generation, 1),
           status: :accepted
         }}

      {:reply, reply, %{state | registration_required?: false}}
    end

    def handle_call({:request, message}, _from, %{registration_required?: true} = state) do
      send(state.owner, {:request_before_resume, message})
      {:reply, {:error, :runner_session_not_found}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{} = request}, _from, state) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call(
          {:request, %RunnerTask.Result{} = result},
          _from,
          %{fail_next: :result} = state
        ) do
      send(state.owner, {:result_interrupted, result})

      {:reply, {:error, :control_plane_unavailable},
       %{state | fail_next: nil, registration_required?: true}}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_resumed, result})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    def handle_call(
          {:request, %RunnerTask.LeaseRenewal{} = renewal},
          _from,
          %{fail_next: :renewal} = state
        ) do
      send(state.owner, {:renewal_interrupted, renewal})

      {:reply, {:error, :control_plane_unavailable},
       %{state | fail_next: nil, registration_required?: true}}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:renewal_resumed, renewal})
      {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule BlockingReregistrationControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)
    def block_next_registration(server), do: GenServer.call(server, :block_next_registration)
    def release_registration(server), do: GenServer.cast(server, :release_registration)

    @impl true
    def init(owner),
      do: {:ok, %{owner: owner, block_registration?: false, pending_registration: nil}}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call(:block_next_registration, _from, state),
      do: {:reply, :ok, %{state | block_registration?: true}}

    def handle_call(
          {:register, registration, agent},
          from,
          %{block_registration?: true} = state
        ) do
      send(state.owner, {:reregistration_blocked, registration, agent})

      {:noreply,
       %{state | block_registration?: false, pending_registration: {from, registration}}}
    end

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:blocking_registration_accepted, registration, agent})
      {:reply, registration_ack(registration), state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{} = request}, _from, state) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:renewal_during_reregistration, renewal})
      {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}
    end

    def handle_call({:request, %RunnerTask.LogBatch{} = batch}, _from, state) do
      ack = %RunnerTask.LogAck{
        workspace_id: batch.workspace_id,
        task_id: batch.task_id,
        runner_instance_id: batch.runner_instance_id,
        runner_session_generation: batch.runner_session_generation,
        assignment_generation: batch.assignment_generation,
        batch_id: batch.batch_id,
        sequence: batch.sequence
      }

      {:reply, {:ok, ack}, state}
    end

    @impl true
    def handle_cast(
          :release_registration,
          %{pending_registration: {from, registration}} = state
        ) do
      GenServer.reply(from, registration_ack(registration))
      {:noreply, %{state | pending_registration: nil}}
    end

    defp registration_ack(registration) do
      {:ok,
       %RunnerTask.RegistrationAck{
         runner_instance_id: registration.runner_instance_id,
         runner_session_generation: max(registration.runner_session_generation, 1),
         status: :accepted
       }}
    end
  end

  defmodule ExecutingControlPlane do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
    def block_logs(server), do: GenServer.call(server, :block_logs)

    @impl true
    def init(state), do: {:ok, Map.put(state, :assignment, nil)}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call(:block_logs, _from, state),
      do: {:reply, :ok, Map.put(state, :block_logs?, true)}

    def handle_call({:register, registration, agent}, _from, state) do
      assignment = %RunnerTask.Assignment{
        command_id: "execute-claim",
        workspace_id: "workspace-execute",
        task_id: "rt_" <> state.work.run_id,
        task_kind: :asset_attempt,
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        assignment_generation: 1,
        runner_pool: "duckdb",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        assigned_at: DateTime.utc_now(),
        lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
        retry_class: :unknown_do_not_retry,
        payload: state.work
      }

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, Map.merge(state, %{assignment: assignment, agent: agent})}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: nil} = state
        ) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call(
          {:request, %RunnerTask.Started{} = started},
          from,
          %{block_second_started_ack?: true, started_count: count} = state
        )
        when count > 0 do
      send(state.owner, {:started_after_preparation, started})
      {:noreply, %{state | started_count: count + 1, started_from: from}}
    end

    def handle_call({:request, %RunnerTask.Started{} = started}, _from, state) do
      send(state.owner, {:task_started, started, state.agent})
      {:reply, {:ok, %{status: :running}}, state}
    end

    def handle_call({:request, %RunnerTask.CancellationAck{} = ack}, _from, state),
      do: {:reply, {:ok, ack}, state}

    def handle_call({:request, %RunnerTask.LogBatch{} = batch}, _from, state) do
      send(state.owner, {:log_batch_persisted, batch})

      if Map.get(state, :block_logs?, false) do
        send(state.owner, :log_delivery_blocked)

        receive do
          :release_logs -> :ok
        end
      end

      ack = %RunnerTask.LogAck{
        workspace_id: batch.workspace_id,
        task_id: batch.task_id,
        runner_instance_id: batch.runner_instance_id,
        runner_session_generation: batch.runner_session_generation,
        assignment_generation: batch.assignment_generation,
        batch_id: batch.batch_id,
        sequence: batch.sequence
      }

      {:reply, {:ok, ack}, Map.put(state, :block_logs?, false)}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:cancelled_result_persisted, result})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | assignment: nil}}
    end
  end

  defmodule BlockingPreparationControlPlane do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state) do
      {:ok,
       state
       |> Map.put(:assignment, nil)
       |> Map.put(:preparation_released?, false)
       |> Map.put(:started_count, 0)}
    end

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      assignment =
        state.assignment ||
          %RunnerTask.Assignment{
            command_id: "blocked-preparation-claim",
            workspace_id: "workspace-blocked-preparation",
            task_id: "rt_blocked_preparation_#{System.unique_integer([:positive, :monotonic])}",
            task_kind: :asset_attempt,
            runner_instance_id: registration.runner_instance_id,
            runner_session_generation: 1,
            assignment_generation: 1,
            runner_pool: "duckdb",
            required_runner_release_id: FavnTestSupport.runner_release_id(),
            assigned_at: DateTime.utc_now(),
            lease_expires_at: DateTime.add(DateTime.utc_now(), 150, :millisecond),
            retry_class: :unknown_do_not_retry,
            payload: state.work
          }

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, Map.merge(state, %{assignment: assignment, agent: agent})}
    end

    def handle_call(
          {:request, %RunnerTask.ClaimRequest{} = request},
          _from,
          %{assignment: nil} = state
        ) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call(
          {:fetch_manifest, _assignment},
          _from,
          %{preparation_released?: true} = state
        ),
        do: {:reply, {:ok, state.version}, state}

    def handle_call({:fetch_manifest, _assignment}, from, state) do
      send(state.owner, :preparation_blocked)
      {:noreply, Map.put(state, :fetch_from, from)}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:renewed_during_preparation, renewal})
      {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}
    end

    def handle_call({:request, %RunnerTask.Started{} = started}, _from, state) do
      send(state.owner, {:started_after_preparation, started})

      if state[:lose_first_started_ack?] and state.started_count == 0 do
        {:reply, {:error, :control_plane_unavailable}, %{state | started_count: 1}}
      else
        {:reply, {:ok, %{status: :running}}, %{state | started_count: state.started_count + 1}}
      end
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | assignment: nil}}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}

    @impl true
    def handle_info(:release_preparation, %{fetch_from: from} = state) do
      GenServer.reply(from, {:ok, state.version})

      {:noreply,
       state
       |> Map.delete(:fetch_from)
       |> Map.put(:preparation_released?, true)}
    end

    def handle_info(:release_second_started, %{started_from: from} = state) do
      GenServer.reply(from, {:ok, %{status: :running}})
      {:noreply, Map.delete(state, :started_from)}
    end
  end

  test "an elastic runner honors the control-plane wait and exits after a final empty claim" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, {self(), 5}})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, %RunnerTask.Registration{lifecycle_mode: :elastic}, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
    assert_receive {:runner_exit, 0}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "blocked assignment preparation cannot block lease renewal" do
    {version, work} = executable_work("blocked_preparation")

    {:ok, control_plane} =
      start_supervised(
        {BlockingPreparationControlPlane, owner: self(), version: version, work: work}
      )

    agent =
      start_supervised!({
        RunnerAgent,
        name: nil,
        connection: control_plane,
        runner_pool: :duckdb,
        lifecycle_mode: :resident,
        exit_fun: fn _status -> :ok end
      })

    assert_receive :preparation_blocked, 1_000

    assert_receive {:renewed_during_preparation, %RunnerTask.LeaseRenewal{}}, 1_000
    send(control_plane, :release_preparation)
    assert_receive {:started_after_preparation, %RunnerTask.Started{}}, 1_000
    assert Process.alive?(agent)
  end

  test "Started preserves its issued-at timestamp after renewal and an acknowledgement loss" do
    {version, work} = executable_work("started_ack_loss")

    {:ok, control_plane} =
      start_supervised(
        {BlockingPreparationControlPlane,
         owner: self(),
         version: version,
         work: work,
         lose_first_started_ack?: true,
         block_second_started_ack?: true}
      )

    agent =
      start_supervised!({
        RunnerAgent,
        name: nil,
        connection: control_plane,
        runner_pool: :duckdb,
        lifecycle_mode: :resident,
        exit_fun: fn _status -> :ok end
      })

    assert_receive :preparation_blocked, 1_000
    assert_receive {:renewed_during_preparation, %RunnerTask.LeaseRenewal{}}, 1_000
    send(control_plane, :release_preparation)

    assert_receive {:started_after_preparation, %RunnerTask.Started{} = first}, 1_000
    assert_receive {:started_after_preparation, %RunnerTask.Started{} = replay}, 2_000
    assert replay.issued_at == first.issued_at
    assert DateTime.compare(replay.occurred_at, first.occurred_at) in [:eq, :gt]
    send(control_plane, :release_second_started)
    assert Process.alive?(agent)
  end

  test "a rejected boot registration exits non-zero" do
    owner = self()
    {:ok, control_plane} = start_supervised({RejectedControlPlane, owner})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:rejected_registration, %RunnerTask.Registration{}, ^agent}
    assert_receive {:runner_exit, 1}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "an elastic runner exits at bounded maximum uptime while idle" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         max_uptime_ms: 10,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, %RunnerTask.Registration{}, ^agent}
    assert_receive {:runner_exit, 0}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "a resident runner waits within its interval until the control plane wakes it" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, {self(), 60_000}})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    refute_receive {:claimed, %RunnerTask.ClaimRequest{}}, 50

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Wake{
         runner_instance_id: registration.runner_instance_id,
         runner_session_generation: 1,
         runner_pool: registration.runner_pool,
         required_runner_release_id: registration.required_runner_release_id
       }}
    )

    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
  end

  test "a resident runner re-claims when its wait interval expires without a wake" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, {self(), 5}})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, %RunnerTask.Registration{}, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
    assert Process.alive?(agent)
  end

  test "cancellation is acknowledged and applied only to the exact assignment fence" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: assignment.workspace_id,
         task_id: assignment.task_id,
         runner_instance_id: assignment.runner_instance_id,
         runner_session_generation: assignment.runner_session_generation,
         assignment_generation: assignment.assignment_generation,
         command_id: "cancel-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :observed}}
    assert_receive {:executor_cancelled, :operator_request}
  end

  test "cancellation is rejected after execution has already reached reporting" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: nil,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: assignment.workspace_id,
         task_id: assignment.task_id,
         runner_instance_id: assignment.runner_instance_id,
         runner_session_generation: assignment.runner_session_generation,
         assignment_generation: assignment.assignment_generation,
         command_id: "cancel-reported-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    refute_receive {:executor_cancelled, _reason}
  end

  test "runtime-input-only asset tasks finish without starting customer execution" do
    assignment = assignment("runner-runtime-input-resolution")

    work = %{
      assignment.payload
      | run_id: "run_runtime_input_resolution",
        asset_step_id: "step_runtime_input_resolution",
        attempt: 1,
        metadata: %{runner_task_mode: :runtime_input_resolution}
    }

    assignment = %{assignment | payload: work}

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: work, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %RunnerResult{
                      run_id: "run_runtime_input_resolution",
                      status: :ok,
                      asset_results: []
                    }}
  end

  test "cancellation racing a terminal executor is rejected without crashing the runner" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FinishedExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, {:favn_runner_task, cancellation(assignment, "cancel-terminal-race")})

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    assert Process.alive?(agent)

    Process.exit(executor, :kill)
    send(agent, {:favn_runner_task, cancellation(assignment, "cancel-dead-executor-race")})

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    assert Process.alive?(agent)
  end

  test "an executor process loss produces one conservative durable result" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, owner})
    {:ok, executor} = start_supervised({FakeExecutor, owner})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          executor_monitor: Process.monitor(executor),
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    Process.exit(executor, :kill)

    assert_receive {:result_delivery,
                    %RunnerTask.Result{
                      task_id: "rt_task",
                      outcome: :unknown,
                      retry_class: :unknown_do_not_retry
                    }}

    # The fenced delivery is abandoned and the resident runner keeps serving
    # instead of exiting.
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
    refute_received {:runner_exit, _status}
    refute_receive {:result_delivery, %RunnerTask.Result{task_id: "rt_task"}}, 100
    assert Process.alive?(agent)
  end

  test "an expired unrenewed lease stops execution without draining a resident runner" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = %{
      assignment(registration.runner_instance_id)
      | lease_expires_at: DateTime.add(DateTime.utc_now(), -1, :second)
    }

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, :renew_lease)

    assert_receive {:lease_renewal, %RunnerTask.LeaseRenewal{}}
    assert_receive {:executor_cancelled, :runner_task_lease_lost}

    state = :sys.get_state(agent)
    assert state.phase == :lease_lost
    refute state.draining?
  end

  test "a stale result fence is abandoned instead of retrying forever" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    result = %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: :unknown,
      retry_class: :unknown_do_not_retry,
      error: Favn.Contracts.RunnerError.new(outcome: :unknown, retryable?: false),
      finished_at: DateTime.utc_now()
    }

    :ok = FavnRunner.TaskResultBuffer.reset()
    :ok = FavnRunner.TaskResultBuffer.put_result(result)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: nil,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(agent, :deliver_result)

    assert_receive {:result_delivery, ^result}

    # The fenced result is dropped and the resident runner claims new work
    # instead of exiting.
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
    assert nil == FavnRunner.TaskResultBuffer.pending_result()
  end

  test "an active assignment re-registers before result delivery resumes" do
    {:ok, control_plane} = start_supervised({RestartingControlPlane, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:restart_registration, registration, ^agent}
    assignment = assignment(registration.runner_instance_id)

    result = %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: :unknown,
      retry_class: :unknown_do_not_retry,
      result: nil,
      error: Favn.Contracts.RunnerError.new(outcome: :unknown),
      finished_at: DateTime.utc_now()
    }

    :ok = FavnRunner.TaskResultBuffer.put_result(result)
    :ok = RestartingControlPlane.fail_next(control_plane, :result)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(agent, :deliver_result)

    assert_receive {:result_interrupted, ^result}

    assert_receive {:restart_registration,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent},
                   2_000

    assert active_assignment.task_id == assignment.task_id
    refute_receive {:request_before_resume, _message}
    assert_receive {:result_resumed, ^result}
    assert_eventually(fn -> FavnRunner.TaskResultBuffer.pending_result() == nil end)
  end

  test "an executing assignment re-registers before lease renewal resumes" do
    {:ok, control_plane} = start_supervised({RestartingControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:restart_registration, registration, ^agent}
    assignment = assignment(registration.runner_instance_id)
    :ok = RestartingControlPlane.fail_next(control_plane, :renewal)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, :renew_lease)

    assert_receive {:renewal_interrupted, %RunnerTask.LeaseRenewal{}}

    assert_receive {:restart_registration,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent},
                   2_000

    assert active_assignment.task_id == assignment.task_id
    refute_receive {:request_before_resume, _message}
    assert_receive {:renewal_resumed, %RunnerTask.LeaseRenewal{}}, 2_000
  end

  test "a blocked active-assignment registration does not block lease renewal" do
    {:ok, control_plane} = start_supervised({BlockingReregistrationControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:blocking_registration_accepted, registration, ^agent}
    assignment = assignment(registration.runner_instance_id)
    :ok = BlockingReregistrationControlPlane.block_next_registration(control_plane)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :connecting,
          resume_phase: :running
      }
    end)

    send(agent, :connect)

    assert_receive {:reregistration_blocked,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent}

    assert active_assignment.task_id == assignment.task_id

    send(agent, :renew_lease)
    assert_receive {:renewal_during_reregistration, %RunnerTask.LeaseRenewal{}}, 1_000

    BlockingReregistrationControlPlane.release_registration(control_plane)
    assert_eventually(fn -> :sys.get_state(agent).phase == :running end)
  end

  test "lease deadline wins a late active-assignment registration acknowledgement" do
    {:ok, control_plane} = start_supervised({BlockingReregistrationControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:blocking_registration_accepted, registration, ^agent}

    assignment = %{
      assignment(registration.runner_instance_id)
      | lease_expires_at: DateTime.add(DateTime.utc_now(), -1, :second)
    }

    :ok = BlockingReregistrationControlPlane.block_next_registration(control_plane)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :connecting,
          resume_phase: :running
      }
    end)

    send(agent, :connect)

    assert_receive {:reregistration_blocked,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent}

    assert active_assignment.task_id == assignment.task_id

    send(
      agent,
      {:lease_deadline, assignment.task_id, assignment.assignment_generation,
       assignment.lease_expires_at}
    )

    assert_receive {:executor_cancelled, :runner_task_lease_lost}
    assert :sys.get_state(agent).phase == :lease_lost

    BlockingReregistrationControlPlane.release_registration(control_plane)

    assert_eventually(fn ->
      state = :sys.get_state(agent)
      state.phase == :lease_lost and is_nil(state.resume_phase)
    end)

    refute :sys.get_state(agent).phase == :running
  end

  test "runtime input resolution replays the exact committed payload after a lost acknowledgement" do
    :ok = FavnRunner.TaskResultBuffer.reset()
    {version, work} = runtime_input_version_and_work()
    {:ok, resolver_calls} = Agent.start_link(fn -> 0 end)

    resolver = fn _work ->
      call = Agent.get_and_update(resolver_calls, fn count -> {count + 1, count + 1} end)

      Favn.RuntimeInput.Resolution.new(%{
        resolver: __MODULE__,
        params: %{region: if(call == 1, do: "eu", else: "us")},
        input_identity: "settings-#{call}",
        metadata: %{},
        sensitive_params: []
      })
    end

    {:ok, control_plane} =
      start_supervised(
        {RuntimeInputReplayControlPlane, owner: self(), version: version, work: work}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: resolver,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:runtime_input_registration, %RunnerTask.Registration{}, ^agent}

    assert_receive {:runtime_inputs_committed_before_ack,
                    %RunnerTask.RuntimeInputsResolved{} = first},
                   2_000

    assert_receive {:runtime_input_registration,
                    %RunnerTask.Registration{active_assignment: active}, ^agent},
                   2_000

    assert active.task_id == first.task_id
    assert_receive {:runtime_inputs_replayed, %RunnerTask.RuntimeInputsResolved{} = replay}, 2_000
    assert replay == first
    assert Agent.get(resolver_calls, & &1) == 1
  end

  test "real task executor cancellation reaches a valid persisted acknowledgement" do
    :ok = FavnRunner.TaskResultBuffer.reset()

    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_task_executor_cancel_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_task_executor_cancel",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_task_executor_cancel",
      attempt: 1,
      metadata: %{}
    }

    {:ok, control_plane} =
      start_supervised({ExecutingControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:task_started, %RunnerTask.Started{} = started, ^agent}, 2_000
    assert_eventually(fn -> is_pid(:sys.get_state(agent).executor) end)
    executor = :sys.get_state(agent).executor

    :ok = ExecutingControlPlane.block_logs(control_plane)

    Enum.each(1..50, fn index ->
      assert :ok =
               GenServer.call(
                 executor,
                 {:runner_log_entry, started.task_id,
                  %{message: "#{index}:" <> String.duplicate("x", 8_192)}}
               )
    end)

    assert_receive :log_delivery_blocked

    Enum.each(1..500, fn index ->
      assert :ok =
               GenServer.call(
                 executor,
                 {:runner_log_entry, started.task_id, %{message: "blocked-#{index}"}}
               )
    end)

    assert %{count: count, dropped: dropped, max_entries: max_entries} =
             FavnRunner.TaskResultBuffer.stats()

    assert count == max_entries
    assert dropped > 0
    assert {:message_queue_len, queue_len} = Process.info(agent, :message_queue_len)
    assert queue_len <= 2

    send(control_plane, :release_logs)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: started.workspace_id,
         task_id: started.task_id,
         runner_instance_id: started.runner_instance_id,
         runner_session_generation: started.runner_session_generation,
         assignment_generation: started.assignment_generation,
         command_id: "cancel-real-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancelled_result_persisted, %RunnerTask.Result{} = result}, 2_000
    assert result.outcome == :unknown, inspect(result)
    assert result.retry_class == :unknown_do_not_retry
    assert result.error.type == :native_cancel_unknown
    assert result.error.outcome == :unknown
    assert :ok = RunnerTask.Result.validate(result)

    assert_receive {:log_batch_persisted, first_batch}
    assert_receive {:log_batch_persisted, second_batch}
    assert :ok = RunnerTask.LogBatch.validate(first_batch)
    assert :ok = RunnerTask.LogBatch.validate(second_batch)
    assert first_batch.sequence == 0
    assert second_batch.sequence == 1
  end

  test "killing an asset executor also terminates its owned customer-code worker" do
    :ok = FavnRunner.TaskResultBuffer.reset()
    Application.put_env(:favn_runner, :announcing_asset_owner, self())
    on_exit(fn -> Application.delete_env(:favn_runner, :announcing_asset_owner) end)

    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.AnnouncingAsset, :asset},
      module: __MODULE__.AnnouncingAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_asset_executor_ownership_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_asset_executor_ownership",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_asset_executor_ownership",
      attempt: 1,
      metadata: %{}
    }

    {:ok, control_plane} =
      start_supervised({ExecutingControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:task_started, %RunnerTask.Started{}, ^agent}, 2_000

    # The worker announces itself from inside the asset body, so it is parked in
    # an infinite sleep and cannot exit on its own before the kill below.
    assert_receive {:asset_running, worker}, 2_000
    worker_monitor = Process.monitor(worker)
    refute_receive {:DOWN, ^worker_monitor, :process, ^worker, _reason}, 100

    assert_eventually(fn -> is_pid(:sys.get_state(agent).executor) end)
    executor = :sys.get_state(agent).executor
    assert :sys.get_state(executor).worker == worker

    executor_monitor = Process.monitor(executor)
    Process.exit(executor, :kill)

    assert_receive {:DOWN, ^executor_monitor, :process, ^executor, :killed}
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, reason}
    assert reason in [:killed, :kill]
    refute Process.alive?(worker)
    assert_receive {:cancelled_result_persisted, %RunnerTask.Result{}}, 2_000
    assert_eventually(fn -> is_nil(:sys.get_state(agent).assignment) end)
  end

  test "a resolver failure that arrives with an unknown outcome is still reported acceptably" do
    {version, work} = executable_work("unknown_prep_failure")
    owner = self()

    {:ok, control_plane} =
      start_supervised(
        {PreparationFailureControlPlane, owner: self(), version: version, work: work}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error,
            RunnerError.new(
              type: :runtime_inputs_raised,
              phase: :runtime_inputs,
              message: "runtime input resolver raised",
              retryable?: false,
              outcome: :unknown
            )}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:runtime_inputs_reported, %RunnerTask.RuntimeInputsResolved{status: :failed}},
                   2_000

    assert_receive {:result_delivered, %RunnerTask.Result{} = result, validation}, 2_000
    assert validation == :ok
    assert result.outcome == :unknown
    assert result.retry_class == :unknown_do_not_retry
    assert result.error.type == :runtime_inputs_raised

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "a deterministic safe resolver failure reports failed/terminal and the runner claims on" do
    {version, work} = executable_work("safe_prep_failure")
    owner = self()

    {:ok, control_plane} =
      start_supervised(
        {PreparationFailureControlPlane, owner: self(), version: version, work: work}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error,
            RunnerError.new(
              type: :runtime_inputs_failed,
              phase: :runtime_inputs,
              message: "no completed landing run for the window",
              retryable?: false,
              outcome: :safe_failure
            )}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:result_delivered, %RunnerTask.Result{} = result, validation}, 2_000
    assert validation == :ok
    assert result.outcome == :failed
    assert result.retry_class == :terminal
    assert result.error.type == :runtime_inputs_failed

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "a permanently rejected result is replaced by an accepted unknown fallback" do
    {version, work} = executable_work("rejected_result")
    owner = self()

    {:ok, control_plane} =
      start_supervised(
        {PreparationFailureControlPlane,
         owner: self(), version: version, work: work, reject_results: [%{kind: :invalid}]}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error, RunnerError.new(retryable?: false, outcome: :safe_failure)}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:result_rejected, %RunnerTask.Result{}, %{kind: :invalid}}, 2_000

    assert_receive {:result_delivered, %RunnerTask.Result{} = fallback, validation}, 2_000
    assert validation == :ok
    assert fallback.outcome == :unknown
    assert fallback.retry_class == :unknown_do_not_retry
    assert fallback.error.type == :runner_task_result_rejected

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "a rejected runtime-input resolution fails the preparation instead of resending forever" do
    {version, work} = executable_work("runtime_inputs_rejected")
    owner = self()

    {:ok, control_plane} =
      start_supervised(
        {PreparationFailureControlPlane,
         owner: self(),
         version: version,
         work: work,
         reject_runtime_inputs: {:invalid_runtime_inputs_resolved, :failed}}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error, RunnerError.new(retryable?: false, outcome: :safe_failure)}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:runtime_inputs_rejected, %RunnerTask.RuntimeInputsResolved{}, _reason},
                   2_000

    assert_receive {:result_delivered, %RunnerTask.Result{} = result, validation}, 2_000
    assert validation == :ok
    assert result.outcome == :failed
    assert result.retry_class == :safe_to_retry

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runtime_inputs_rejected, _message, _reason}
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "a rejected fallback abandons the assignment instead of delivering it again" do
    {version, work} = executable_work("rejected_fallback")
    owner = self()

    {:ok, control_plane} =
      start_supervised(
        {PreparationFailureControlPlane,
         owner: self(),
         version: version,
         work: work,
         reject_results: [%{kind: :invalid}, %{kind: :invalid}]}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error, RunnerError.new(retryable?: false, outcome: :safe_failure)}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:result_rejected, %RunnerTask.Result{}, %{kind: :invalid}}, 2_000

    assert_receive {:result_rejected, %RunnerTask.Result{} = fallback, %{kind: :invalid}}, 2_000
    assert fallback.error.type == :runner_task_result_rejected

    # The assignment is abandoned: the next delivered result comes from a fresh
    # attempt at the re-claimed task, not a third try at the dead fallback.
    assert_receive {:result_delivered, %RunnerTask.Result{} = retried, :ok}, 2_000
    assert retried.error.type == :runner_error

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "a resident runner drops a fenced assignment on stale resume and keeps serving" do
    {version, work} = executable_work("stale_resume_resident")
    owner = self()

    {:ok, control_plane} =
      start_supervised({StaleResumeControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work ->
           {:error, RunnerError.new(retryable?: false, outcome: :safe_failure)}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:fresh_registration, %RunnerTask.Registration{}, ^agent}, 2_000
    assert_receive {:result_attempted, %RunnerTask.Result{}}, 2_000
    assert_receive {:resume_rejected, %RunnerTask.Registration{}, ^agent}, 2_000

    assert_receive {:fresh_registration, %RunnerTask.Registration{active_assignment: nil},
                    ^agent},
                   2_000

    assert_receive {:claimed_after_drop, %RunnerTask.ClaimRequest{}}, 2_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  test "an elastic runner still exits when its assignment resume is rejected as stale" do
    {version, work} = executable_work("stale_resume_elastic")
    owner = self()

    {:ok, control_plane} =
      start_supervised({StaleResumeControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         runtime_input_resolver: fn _work ->
           {:error, RunnerError.new(retryable?: false, outcome: :safe_failure)}
         end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:resume_rejected, %RunnerTask.Registration{}, ^agent}, 2_000
    assert_receive {:runner_exit, 1}, 2_000
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "a resident runner reports a lost lease and keeps claiming instead of draining" do
    {version, work} = executable_work("lease_loss_resident")
    owner = self()

    {:ok, control_plane} =
      start_supervised({LeaseLossControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: fn _work -> Process.sleep(60_000) end,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:renewal_failed, %RunnerTask.LeaseRenewal{}}, 2_000

    assert_receive {:result_delivered, %RunnerTask.Result{} = result, validation}, 3_000
    assert validation == :ok
    assert result.outcome == :unknown
    assert result.retry_class == :unknown_do_not_retry
    assert result.error.type == :runner_task_lease_lost

    assert_receive {:claimed_next, %RunnerTask.ClaimRequest{}}, 3_000
    refute_received {:runner_exit, _status}
    assert Process.alive?(agent)
  end

  defp executable_work(suffix) do
    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_#{suffix}_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_#{suffix}",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_#{suffix}",
      attempt: 1,
      metadata: %{}
    }

    {version, work}
  end

  defp assignment(runner_instance_id) do
    %RunnerTask.Assignment{
      command_id: "claim-task",
      workspace_id: "workspace-task",
      task_id: "rt_task",
      task_kind: :asset_attempt,
      runner_instance_id: runner_instance_id,
      runner_session_generation: 1,
      assignment_generation: 2,
      runner_pool: "duckdb",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      assigned_at: DateTime.utc_now(),
      lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
      retry_class: :unknown_do_not_retry,
      payload: %Favn.Contracts.RunnerWork{
        manifest_version_id: "mv_task",
        manifest_content_hash: String.duplicate("a", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id()
      }
    }
  end

  defp cancellation(assignment, command_id) do
    %RunnerTask.Cancellation{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      command_id: command_id,
      reason: :operator_request,
      requested_at: DateTime.utc_now()
    }
  end

  defp runtime_input_version_and_work do
    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_runtime_input_replay_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_runtime_input_replay",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_runtime_input_replay",
      attempt: 1,
      metadata: %{}
    }

    {version, work}
  end

  defp assert_eventually(fun, attempts \\ 50)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defmodule SlowAsset do
    def asset(_context), do: Process.sleep(:infinity)
  end

  # Announces the worker process before parking it, so a test can prove the
  # worker reached the asset body instead of peeking at executor state and
  # racing a worker that may still exit on its own.
  defmodule AnnouncingAsset do
    def asset(_context) do
      case Application.get_env(:favn_runner, :announcing_asset_owner) do
        owner when is_pid(owner) -> send(owner, {:asset_running, self()})
        _other -> :ok
      end

      Process.sleep(:infinity)
    end
  end
end

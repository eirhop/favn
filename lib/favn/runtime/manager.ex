defmodule Favn.Runtime.Manager do
  @moduledoc """
  Async run submission manager.

  The manager owns run admission, initial snapshot persistence, and run process
  startup under `Favn.Runtime.RunSupervisor`.
  """

  use GenServer

  alias Favn.Runtime.Projector
  alias Favn.Runtime.State

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec submit_run(Favn.asset_ref() | [Favn.asset_ref()], keyword()) ::
          {:ok, Favn.run_id()} | {:error, term()}
  def submit_run(target_refs, opts \\ []) when is_list(opts) do
    GenServer.call(__MODULE__, {:submit_run, target_refs, opts}, :infinity)
  end

  @spec cancel_run(Favn.run_id()) ::
          {:ok, :cancelling | :cancelled | :already_terminal}
          | {:error,
             :not_found
             | :invalid_run_id
             | :coordinator_unavailable
             | :timeout_in_progress
             | term()}
  def cancel_run(run_id) when is_binary(run_id) do
    GenServer.call(__MODULE__, {:cancel_run, run_id}, :infinity)
  end

  def cancel_run(_run_id), do: {:error, :invalid_run_id}

  @spec rerun_run(Favn.run_id(), keyword()) :: {:ok, Favn.run_id()} | {:error, term()}
  def rerun_run(run_id, opts \\ [])

  def rerun_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    GenServer.call(__MODULE__, {:rerun_run, run_id, opts}, :infinity)
  end

  def rerun_run(_run_id, _opts), do: {:error, :invalid_run_id}

  @impl true
  def init(_opts), do: {:ok, %{run_monitors: %{}, run_pids: %{}}}

  @impl true
  def handle_call({:submit_run, target_refs, opts}, _from, state) do
    {reply, next_state} = do_submit_run(target_refs, opts, state)
    {:reply, reply, next_state}
  end

  @impl true
  def handle_call({:rerun_run, run_id, opts}, _from, state) do
    {reply, next_state} =
      with {:ok, mode} <- normalize_rerun_mode(opts),
           {:ok, source_run} <- Favn.Storage.get_run(run_id),
           :ok <- ensure_terminal(source_run),
           {:ok, submit_opts} <- build_rerun_submit_opts(source_run, opts, mode) do
        do_submit_run(source_run.target_refs, submit_opts, state)
      else
        {:error, reason} -> {{:error, reason}, state}
      end

    {:reply, reply, next_state}
  end

  @impl true
  def handle_call({:cancel_run, run_id}, _from, state) do
    reply =
      case Favn.Storage.get_run(run_id) do
        {:ok, %{status: :cancelled}} ->
          {:ok, :cancelled}

        {:ok, %{status: :running, terminal_reason: %{kind: :timed_out}}} ->
          {:error, :timeout_in_progress}

        {:ok, %{status: :running}} ->
          with {:ok, pid} <- Map.fetch(state.run_pids, run_id) do
            GenServer.cast(pid, {:cancel_run, %{requested_by: :api}})
            {:ok, :cancelling}
          else
            :error -> {:error, :coordinator_unavailable}
          end

        {:ok, _run} ->
          {:ok, :already_terminal}

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  defp do_submit_run(target_refs, opts, state) do
    dependencies = Keyword.get(opts, :dependencies, :all)
    params = Keyword.get(opts, :params, %{})
    pipeline_context = Keyword.get(opts, :_pipeline_context)
    max_concurrency = resolve_max_concurrency(opts)
    timeout_ms = Keyword.get(opts, :timeout_ms)
    retry_policy = resolve_retry_policy(opts)

    with :ok <- validate_params(params),
         :ok <- validate_pipeline_context(pipeline_context),
         :ok <- validate_max_concurrency(max_concurrency),
         :ok <- validate_timeout_ms(timeout_ms),
         {:ok, retry_policy} <- validate_retry_policy(retry_policy),
         {:ok, plan} <- resolve_plan(target_refs, dependencies, opts),
         runtime_state <-
           build_runtime_state(
             plan,
             params,
             pipeline_context,
             max_concurrency,
             timeout_ms,
             retry_policy,
             opts
           ),
         {:ok, pid} <- start_run_coordinator(runtime_state),
         :ok <- persist_initial_snapshot(runtime_state),
         :ok <- emit_run_created(runtime_state),
         :ok <- kickoff_run(pid) do
      ref = Process.monitor(pid)

      next_state =
        state
        |> put_in([:run_monitors, ref], runtime_state.run_id)
        |> put_in([:run_pids, runtime_state.run_id], pid)

      {{:ok, runtime_state.run_id}, next_state}
    else
      {:start_failed_after_spawn, pid, reason} ->
        _ = DynamicSupervisor.terminate_child(Favn.Runtime.RunSupervisor, pid)
        {{:error, reason}, state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {run_id, remaining} = Map.pop(state.run_monitors, ref)
    run_pids = if run_id, do: Map.delete(state.run_pids, run_id), else: state.run_pids

    if run_id do
      maybe_finalize_crashed_run(run_id, reason)
    end

    {:noreply, %{state | run_monitors: remaining, run_pids: run_pids}}
  end

  defp build_runtime_state(
         plan,
         params,
         pipeline_context,
         max_concurrency,
         timeout_ms,
         retry_policy,
         opts
       ) do
    submit_kind = Keyword.get(opts, :_submit_kind, :asset)
    submit_ref = Keyword.get(opts, :_submit_ref)
    replay_mode = Keyword.get(opts, :_replay_mode, :none)
    rerun_of_run_id = Keyword.get(opts, :_rerun_of_run_id)
    parent_run_id = Keyword.get(opts, :_parent_run_id)
    root_run_id = Keyword.get(opts, :_root_run_id)
    lineage_depth = Keyword.get(opts, :_lineage_depth, 0)
    operator_reason = Keyword.get(opts, :_operator_reason)

    %State{
      run_id: new_run_id(),
      target_refs: plan.target_refs,
      target_node_keys: plan.target_node_keys,
      plan: plan,
      params: params,
      pipeline_context: pipeline_context,
      max_concurrency: max_concurrency,
      timeout_ms: timeout_ms,
      retry_policy: retry_policy,
      submit_kind: submit_kind,
      submit_ref: submit_ref,
      replay_mode: replay_mode,
      rerun_of_run_id: rerun_of_run_id,
      parent_run_id: parent_run_id,
      root_run_id: root_run_id,
      lineage_depth: lineage_depth,
      operator_reason: operator_reason,
      event_seq: 1,
      steps: build_steps(plan, retry_policy, Keyword.get(opts, :_resume_successful_steps, %{}))
    }
  end

  defp build_steps(plan, retry_policy, resume_successful_steps) do
    plan.nodes
    |> Enum.reduce(%{}, fn {node_key, node}, acc ->
      ref = node.ref

      step =
        case Map.fetch(resume_successful_steps, ref) do
          {:ok, result} ->
            %Favn.Runtime.StepState{
              ref: ref,
              node_key: node_key,
              runtime_window: node.window,
              stage: node.stage,
              upstream: node.upstream,
              downstream: node.downstream,
              status: :success,
              attempt: result.attempt_count,
              max_attempts: result.max_attempts,
              started_at: result.started_at,
              finished_at: result.finished_at,
              duration_ms: result.duration_ms,
              meta: result.meta || %{},
              attempts: result.attempts || []
            }

          :error ->
            status = if node.upstream == [], do: :ready, else: :pending

            %Favn.Runtime.StepState{
              ref: ref,
              node_key: node_key,
              runtime_window: node.window,
              stage: node.stage,
              upstream: node.upstream,
              downstream: node.downstream,
              status: status,
              max_attempts: retry_policy.max_attempts
            }
        end

      Map.put(acc, node_key, step)
    end)
    |> promote_ready_steps_from_restored_success(plan)
  end

  defp promote_ready_steps_from_restored_success(steps, %Favn.Plan{} = plan) do
    Enum.reduce(stage_node_keys(plan), steps, fn stage_node_keys, acc ->
      Enum.reduce(stage_node_keys, acc, fn node_key, stage_acc ->
        step = Map.fetch!(stage_acc, node_key)

        cond do
          step.status != :pending ->
            stage_acc

          Enum.all?(step.upstream, fn upstream_key ->
            Map.fetch!(stage_acc, upstream_key).status == :success
          end) ->
            Map.update!(stage_acc, node_key, &%{&1 | status: :ready})

          true ->
            stage_acc
        end
      end)
    end)
  end

  defp stage_node_keys(%Favn.Plan{node_stages: node_stages})
       when is_list(node_stages) and node_stages != [],
       do: node_stages

  defp stage_node_keys(%Favn.Plan{stages: stages}) do
    Enum.map(stages, fn stage_refs -> Enum.map(stage_refs, &{&1, nil}) end)
  end

  defp persist_initial_snapshot(%State{} = runtime_state) do
    case runtime_state |> Projector.to_public_run() |> Favn.Storage.put_run() do
      :ok -> :ok
      {:error, reason} -> {:error, {:storage_persist_failed, reason}}
    end
  end

  defp emit_run_created(%State{} = runtime_state) do
    _ =
      Favn.Runtime.Telemetry.emit_runtime_event(:run_created, %{
        run_id: runtime_state.run_id,
        seq: 1,
        entity: :run,
        status: runtime_state.run_status,
        data: %{}
      })

    Favn.Runtime.Events.publish_run_event(runtime_state.run_id, :run_created, %{
      seq: 1,
      entity: :run,
      status: runtime_state.run_status,
      data: %{
        submit_kind: runtime_state.submit_kind,
        replay_mode: runtime_state.replay_mode,
        rerun_of_run_id: runtime_state.rerun_of_run_id,
        parent_run_id: runtime_state.parent_run_id,
        root_run_id: runtime_state.root_run_id
      }
    })

    :ok
  end

  defp kickoff_run(pid) when is_pid(pid) do
    GenServer.cast(pid, :start_run)
    :ok
  rescue
    error ->
      {:start_failed_after_spawn, pid, error}
  end

  defp start_run_coordinator(%State{} = runtime_state) do
    child_spec = %{
      id: {Favn.Runtime.Coordinator, runtime_state.run_id},
      start: {Favn.Runtime.Coordinator, :start_link, [[state: runtime_state]]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    case DynamicSupervisor.start_child(Favn.Runtime.RunSupervisor, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_finalize_crashed_run(run_id, reason) do
    with {:ok, run} <- Favn.Storage.get_run(run_id),
         true <- run.status in [:running] do
      failed =
        %{
          run
          | status: :error,
            event_seq: run.event_seq + 1,
            finished_at: DateTime.utc_now(),
            error: {:run_process_crash, reason}
        }

      case Favn.Storage.put_run(failed) do
        :ok ->
          _ =
            Favn.Runtime.Telemetry.emit_runtime_event(:run_failed, %{
              run_id: run_id,
              seq: failed.event_seq,
              entity: :run,
              status: failed.status,
              data: %{
                duration_ms: run_duration_ms(failed),
                error: failed.error,
                error_class: :run_process_crash,
                error_kind: :exit
              }
            })

          _ =
            Favn.Runtime.Events.publish_run_event(run_id, :run_failed, %{
              seq: failed.event_seq,
              entity: :run,
              status: failed.status,
              data: %{error: failed.error}
            })

          :ok

        {:error, _reason} ->
          :ok
      end

      :ok
    else
      _ -> :ok
    end
  end

  defp ensure_terminal(%Favn.Run{status: status})
       when status in [:ok, :error, :cancelled, :timed_out],
       do: :ok

  defp ensure_terminal(%Favn.Run{status: :running}), do: {:error, :run_not_terminal}

  defp normalize_rerun_mode(opts) do
    case Keyword.get(opts, :mode, :resume_from_failure) do
      :resume_from_failure -> {:ok, :resume_from_failure}
      :exact_replay -> {:ok, :exact_replay}
      invalid -> {:error, {:invalid_rerun_mode, invalid}}
    end
  end

  defp build_rerun_submit_opts(%Favn.Run{} = source_run, opts, mode) do
    with {:ok, plan} <- ensure_replay_plan(source_run) do
      resume_successful_steps = build_resume_successful_steps(source_run, plan, mode)
      parent_depth = source_run.lineage_depth || 0
      root_run_id = source_run.root_run_id || source_run.id

      {:ok,
       [
         dependencies: plan.dependencies,
         params: source_run.params || %{},
         _pipeline_context: source_run.pipeline_context || source_run.pipeline,
         max_concurrency: source_run.max_concurrency || 1,
         timeout_ms: source_run.timeout_ms,
         retry: source_run.retry_policy || %{},
         _plan_override: plan,
         _resume_successful_steps: resume_successful_steps,
         _submit_kind: :rerun,
         _submit_ref: source_run.submit_ref || source_run.target_refs,
         _replay_mode: mode,
         _rerun_of_run_id: source_run.id,
         _parent_run_id: source_run.id,
         _root_run_id: root_run_id,
         _lineage_depth: parent_depth + 1,
         _operator_reason: Keyword.get(opts, :reason)
       ]}
    end
  end

  defp ensure_replay_plan(%Favn.Run{plan: %Favn.Plan{} = plan}), do: {:ok, plan}
  defp ensure_replay_plan(_), do: {:error, :replay_unavailable}

  defp resolve_plan(target_refs, dependencies, opts) when is_list(opts) do
    case Keyword.fetch(opts, :_plan_override) do
      {:ok, %Favn.Plan{} = plan} ->
        {:ok, plan}

      {:ok, _} ->
        {:error, :invalid_plan_override}

      :error ->
        planner_opts = [
          dependencies: dependencies,
          anchor_window: resolve_anchor_window(opts)
        ]

        Favn.plan_asset_run(target_refs, planner_opts)
    end
  end

  defp resolve_anchor_window(opts) do
    case Keyword.get(opts, :_pipeline_context) do
      %{anchor_window: %Favn.Window.Anchor{} = anchor_window} -> anchor_window
      _ -> Keyword.get(opts, :anchor_window)
    end
  end

  defp build_resume_successful_steps(_source_run, _plan, :exact_replay), do: %{}

  defp build_resume_successful_steps(source_run, %Favn.Plan{} = plan, :resume_from_failure) do
    planned_refs = plan.nodes |> Map.values() |> Enum.map(& &1.ref) |> MapSet.new()

    Enum.reduce(source_run.asset_results || %{}, %{}, fn {ref, result}, acc ->
      if result.status == :ok and MapSet.member?(planned_refs, ref) do
        Map.put(acc, ref, result)
      else
        acc
      end
    end)
  end

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_), do: {:error, :invalid_run_params}

  defp validate_pipeline_context(nil), do: :ok
  defp validate_pipeline_context(value) when is_map(value), do: :ok
  defp validate_pipeline_context(_), do: {:error, :invalid_pipeline_context}

  defp run_duration_ms(run) do
    case {run.started_at, run.finished_at} do
      {%DateTime{} = started_at, %DateTime{} = finished_at} ->
        max(DateTime.diff(finished_at, started_at, :millisecond), 0)

      _ ->
        nil
    end
  end

  defp validate_max_concurrency(value) when is_integer(value) and value > 0, do: :ok
  defp validate_max_concurrency(_), do: {:error, :invalid_max_concurrency}

  defp validate_timeout_ms(nil), do: :ok
  defp validate_timeout_ms(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout_ms(_), do: {:error, :invalid_timeout_ms}

  defp resolve_max_concurrency(opts) do
    Keyword.get(opts, :max_concurrency, Application.get_env(:favn, :runtime_max_concurrency, 1))
  end

  defp resolve_retry_policy(opts) do
    configured = Application.get_env(:favn, :runtime_retry, false)
    normalize_retry_policy(Keyword.get(opts, :retry, configured))
  end

  defp normalize_retry_policy(false), do: %{max_attempts: 1, delay_ms: 0, retry_on: []}

  defp normalize_retry_policy(true) do
    %{
      max_attempts: 3,
      delay_ms: 0,
      retry_on: [:exception, :exit, :throw, :timeout, :executor_error]
    }
  end

  defp normalize_retry_policy(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy = Enum.into(policy, %{})
      normalize_retry_policy(policy)
    else
      :invalid_retry_policy
    end
  end

  defp normalize_retry_policy(policy) when is_map(policy) do
    defaults = normalize_retry_policy(true)
    Map.merge(defaults, policy)
  end

  defp normalize_retry_policy(_), do: :invalid_retry_policy

  defp validate_retry_policy(:invalid_retry_policy), do: {:error, :invalid_retry_policy}

  defp validate_retry_policy(%{max_attempts: max_attempts})
       when not (is_integer(max_attempts) and max_attempts > 0),
       do: {:error, :invalid_retry_max_attempts}

  defp validate_retry_policy(%{delay_ms: delay_ms})
       when not (is_integer(delay_ms) and delay_ms >= 0),
       do: {:error, :invalid_retry_delay_ms}

  defp validate_retry_policy(%{retry_on: retry_on})
       when not is_list(retry_on),
       do: {:error, :invalid_retry_retry_on}

  defp validate_retry_policy(%{retry_on: retry_on} = policy) do
    valid = [:exception, :exit, :throw, :timeout, :executor_error, :error_return]

    if Enum.all?(retry_on, &(&1 in valid)) do
      {:ok, %{policy | retry_on: Enum.uniq(retry_on)}}
    else
      {:error, :invalid_retry_retry_on}
    end
  end

  defp new_run_id do
    binary = :crypto.strong_rand_bytes(16)
    <<a::32, b::16, c::16, d::16, e::48>> = binary

    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    Enum.join(
      [
        a |> Integer.to_string(16) |> String.pad_leading(8, "0"),
        b |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        c |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        d |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        e |> Integer.to_string(16) |> String.pad_leading(12, "0")
      ],
      "-"
    )
  end
end

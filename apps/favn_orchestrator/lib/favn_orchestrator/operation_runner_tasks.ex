defmodule FavnOrchestrator.OperationRunnerTasks do
  @moduledoc """
  Durable task boundary for non-asset runner operations.

  Domain owners persist their continuation before calling `ensure/7`. The
  runner task is then an idempotent execution record, not a replacement for the
  owning inspection, generation, or rebuild state machine.
  """

  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Manifest.Index
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunnerTask
  alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerTaskResultRouter
  alias FavnOrchestrator.RunnerTasks

  @default_timeout_ms 300_000
  @operation_poll_ms 250
  @terminal_statuses [:succeeded, :failed, :cancelled, :unknown]

  @capabilities %{
    relation_inspection: "relation_inspection",
    generation_capabilities: "generation_capabilities",
    generation_marker_read: "generation_marker_read",
    generation_marker_initialize: "generation_marker_initialize",
    generation_activate: "generation_activate",
    generation_reconcile: "generation_reconcile",
    generation_discard: "generation_discard"
  }

  @doc "Idempotently ensures one pinned non-asset runner task exists."
  @spec ensure(
          WorkspaceContext.t(),
          Version.t(),
          Favn.Ref.t(),
          atom(),
          struct(),
          term(),
          keyword()
        ) :: {:ok, FavnOrchestrator.Persistence.Results.RunnerTask.t()} | {:error, term()}
  def ensure(
        %WorkspaceContext{} = context,
        %Version{} = version,
        asset_ref,
        task_kind,
        payload,
        domain_identity,
        opts \\ []
      )
      when is_tuple(asset_ref) and is_atom(task_kind) and is_list(opts) do
    task_id = task_id(context.workspace_id, task_kind, domain_identity, version)

    with {:ok, existing_task} <- existing_task(context, task_id) do
      occurred_at =
        (existing_task && existing_task.enqueued_at) ||
          Keyword.get(opts, :occurred_at, DateTime.utc_now())

      issued_at = Keyword.get(opts, :issued_at, occurred_at)

      deadline_at =
        (existing_task && existing_task.deadline_at) || Keyword.get(opts, :deadline_at)

      with :ok <- operation_allows_task(context, Keyword.get(opts, :rebuild_operation_id)),
           {:ok, runner_pool, release_id} <- task_binding(version, asset_ref, opts),
           :ok <- validate_payload_release(payload, release_id),
           {:ok, encoded_payload, payload_hash} <-
             PersistenceCodec.encode_payload(task_kind, payload),
           {:ok, orchestration_context} <-
             PersistenceCodec.encode_orchestration_context(
               Keyword.get(opts, :orchestration_context, %{})
             ),
           {:ok, required_capability} <- Map.fetch(@capabilities, task_kind) do
        with {:ok, _enqueue_receipt} <-
               RunnerTasks.enqueue(%EnqueueRunnerTask{
                 workspace_context: context,
                 command_id: "enqueue:#{task_id}",
                 task_id: task_id,
                 domain_identity: durable_domain_identity(task_kind, domain_identity, version),
                 task_kind: task_kind,
                 runner_pool: runner_pool,
                 required_runner_release_id: release_id,
                 retry_class: RunnerTask.default_retry_class(task_kind),
                 payload: encoded_payload,
                 payload_hash: payload_hash,
                 orchestration_context: orchestration_context,
                 operation_id: Keyword.get(opts, :operation_id),
                 required_capability: required_capability,
                 deadline_at: deadline_at,
                 issued_at: issued_at,
                 occurred_at: occurred_at
               }),
             {:ok, task} <- fetch(context, task_id) do
          maybe_retry_safe(context, task)
        end
      else
        :error -> {:error, {:unsupported_runner_task_kind, task_kind}}
        {:error, _reason} = error -> error
      end
    end
  end

  @doc "Returns the target-owning asset's exact logical pool and frozen release."
  @spec binding(Version.t(), Favn.Ref.t()) ::
          {:ok, %{runner_pool: String.t(), required_runner_release_id: String.t()}}
          | {:error, term()}
  def binding(%Version{} = version, asset_ref) when is_tuple(asset_ref) do
    with {:ok, runner_pool, release_id} <- pool_release(version, asset_ref) do
      {:ok,
       %{
         runner_pool: runner_pool,
         required_runner_release_id: release_id
       }}
    end
  end

  def binding(%Version{} = version, %Asset{} = asset) do
    pool = asset.runner_pool || Favn.RunnerPool.default()

    with {:ok, runner_pool} <- Favn.RunnerPool.encode(pool),
         {:ok, release_id} <- Version.release_for_pool(version, pool) do
      {:ok,
       %{
         runner_pool: runner_pool,
         required_runner_release_id: release_id
       }}
    end
  end

  @doc "Reads one durable operation task by its workspace-scoped identity."
  @spec fetch(WorkspaceContext.t(), String.t()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunnerTask.t()} | {:error, term()}
  def fetch(%WorkspaceContext{} = context, task_id) when is_binary(task_id) do
    FavnOrchestrator.Persistence.stores().runner_tasks.get(%GetRunnerTask{
      workspace_context: context,
      task_id: task_id
    })
  end

  @doc "Waits for an existing task's durable terminal result with a bounded timeout."
  @spec await(WorkspaceContext.t(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def await(%WorkspaceContext{} = context, task_id, opts \\ [])
      when is_binary(task_id) and is_list(opts) do
    timeout_ms = Keyword.get(opts, :timeout, @default_timeout_ms)

    with :ok <- validate_timeout(timeout_ms),
         {:ok, task} <- fetch(context, task_id) do
      if task.status in @terminal_statuses do
        terminal_result(task)
      else
        await_result(context, task_id, timeout_ms, Keyword.get(opts, :rebuild_operation_id))
      end
    end
  end

  @doc "Idempotently ensures a task, then waits for its durable terminal result."
  @spec ensure_and_await(
          WorkspaceContext.t(),
          Version.t(),
          Favn.Ref.t(),
          atom(),
          struct(),
          term(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def ensure_and_await(
        context,
        version,
        asset_ref,
        task_kind,
        payload,
        domain_identity,
        opts \\ []
      ) do
    with {:ok, task} <-
           ensure(context, version, asset_ref, task_kind, payload, domain_identity, opts) do
      await(context, task.task_id, opts)
    end
  end

  @doc false
  def task_id(workspace_id, task_kind, domain_identity, %Version{} = version) do
    digest =
      digest({
        workspace_id,
        task_kind,
        domain_identity,
        version.manifest_version_id,
        version.content_hash
      })

    "rt_" <> digest
  end

  defp await_result(context, task_id, timeout_ms, operation_id) do
    workspace_id = context.workspace_id
    caller = self()

    case Task.Supervisor.start_child(FavnOrchestrator.RunnerTaskWaitSupervisor, fn ->
           RunnerTaskResultRouter.await(workspace_id, task_id, caller)
         end) do
      {:ok, waiter} ->
        monitor = Process.monitor(waiter)
        deadline = System.monotonic_time(:millisecond) + timeout_ms

        await_message(context, task_id, operation_id, waiter, monitor, deadline)

      {:error, reason} ->
        {:error, {:runner_task_waiter_unavailable, reason}}
    end
  catch
    :exit, reason -> {:error, {:runner_task_waiter_unavailable, reason}}
  end

  defp await_message(context, task_id, operation_id, waiter, monitor, deadline) do
    workspace_id = context.workspace_id
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)
    wait_ms = min(remaining, @operation_poll_ms)

    receive do
      {:runner_task_result, ^workspace_id, ^task_id, task} ->
        Process.demonitor(monitor, [:flush])
        terminal_result(task)

      {:DOWN, ^monitor, :process, ^waiter, reason} ->
        {:error, {:runner_task_waiter_stopped, reason}}
    after
      wait_ms ->
        cond do
          remaining == 0 ->
            stop_waiter(waiter, monitor)
            {:error, :runner_task_timeout}

          operation_cancelled?(context, operation_id) ->
            _ = RunnerTasks.request_cancellation(workspace_id, task_id, :rebuild_cancelled)
            stop_waiter(waiter, monitor)
            {:error, :rebuild_operation_cancelled}

          true ->
            await_message(context, task_id, operation_id, waiter, monitor, deadline)
        end
    end
  end

  defp stop_waiter(waiter, monitor) do
    _ =
      Task.Supervisor.terminate_child(
        FavnOrchestrator.RunnerTaskWaitSupervisor,
        waiter
      )

    Process.demonitor(monitor, [:flush])
  end

  defp terminal_result(%{status: :succeeded, result: result}), do: {:ok, result}
  defp terminal_result(%{status: :cancelled, error: nil}), do: {:error, :runner_task_cancelled}
  defp terminal_result(%{status: status, error: error}), do: {:error, {status, error}}

  defp maybe_retry_safe(
         context,
         %{status: :failed, retry_class: :safe_to_retry, result_version: version} = task
       )
       when is_integer(version),
       do: RunnerTasks.retry_safe(context, task)

  defp maybe_retry_safe(_context, task), do: {:ok, task}

  defp existing_task(context, task_id) do
    case fetch(context, task_id) do
      {:ok, task} -> {:ok, task}
      {:error, %FavnOrchestrator.Persistence.Error{kind: :not_found}} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp operation_allows_task(_context, nil), do: :ok

  defp operation_allows_task(context, operation_id) do
    if operation_cancelled?(context, operation_id),
      do: {:error, :rebuild_operation_cancelled},
      else: :ok
  end

  defp operation_cancelled?(_context, nil), do: false

  defp operation_cancelled?(context, operation_id) do
    case FavnOrchestrator.Persistence.stores().rebuilds.get(%GetRebuild{
           workspace_context: context,
           operation_id: operation_id
         }) do
      {:ok, %{state: state}} when state in [:cancelling, :cancelled] -> true
      _other -> false
    end
  end

  defp pool_release(%Version{} = version, asset_ref) do
    with {:ok, index} <- Index.build_from_version(version),
         {:ok, asset} <- Index.fetch_asset(index, asset_ref),
         pool <- asset.runner_pool || Favn.RunnerPool.default(),
         {:ok, pool_name} <- Favn.RunnerPool.encode(pool),
         {:ok, release_id} <- Version.release_for_pool(version, pool) do
      {:ok, pool_name, release_id}
    end
  end

  defp task_binding(version, asset_ref, opts) do
    case Keyword.get(opts, :runner_binding) do
      nil ->
        pool_release(version, asset_ref)

      %{runner_pool: runner_pool, required_runner_release_id: release_id} ->
        with :ok <- Favn.RunnerPool.validate_runtime(runner_pool),
             :ok <- Favn.Contracts.RunnerReleaseBinding.validate(release_id) do
          {:ok, runner_pool, release_id}
        end

      invalid ->
        {:error, {:invalid_runner_task_binding, invalid}}
    end
  end

  defp validate_payload_release(%{required_runner_release_id: release_id}, release_id), do: :ok

  defp validate_payload_release(%{required_runner_release_id: actual}, expected),
    do: {:error, {:runner_task_release_mismatch, expected, actual}}

  defp validate_payload_release(_payload, _release_id), do: :ok

  defp validate_timeout(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout(value), do: {:error, {:invalid_runner_task_timeout, value}}

  defp durable_domain_identity(task_kind, domain_identity, version) do
    "#{task_kind}:" <>
      digest({domain_identity, version.manifest_version_id, version.content_hash})
  end

  defp digest(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end

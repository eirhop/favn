defmodule FavnOrchestrator.Runs do
  @moduledoc """
  Workspace-scoped use cases for authoritative runs and their event stream.

  Callers provide an already-authorized persistence context. This module owns
  command construction and target normalization; concrete stores remain behind
  the persistence capability registry.
  """

  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState

  @doc "Creates one run, its first event, exact targets, ownership root, and outbox event."
  @spec create(WorkspaceContext.t(), RunState.t(), RunEvent.t() | map(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunCommitted.t()}
          | {:error, FavnOrchestrator.Persistence.Error.t() | term()}
  def create(%WorkspaceContext{} = context, %RunState{} = run, event, opts \\ [])
      when is_list(opts) do
    with :ok <- validate_opts(opts, [:command_id, :idempotency, :pipeline_refs]),
         true <- run.workspace_id == context.workspace_id,
         deployment_id when is_binary(deployment_id) and deployment_id != "" <- run.deployment_id,
         {:ok, targets} <- run_targets(run, Keyword.get(opts, :pipeline_refs, [])),
         {:ok, event} <- event_map(event) do
      Persistence.stores().runs.create_run(%CreateRun{
        workspace_context: context,
        command_id: Keyword.get(opts, :command_id) || command_id("create", run),
        deployment_id: deployment_id,
        run: run,
        targets: targets,
        event: event,
        idempotency: Keyword.get(opts, :idempotency)
      })
    else
      false -> {:error, :run_workspace_mismatch}
      nil -> {:error, :run_deployment_required}
      "" -> {:error, :run_deployment_required}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_run_create}
    end
  end

  @doc "Commits one sequence-guarded and optionally owner-fenced run transition."
  @spec commit(WorkspaceContext.t(), RunState.t(), RunEvent.t() | map(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunCommitted.t()}
          | {:error, FavnOrchestrator.Persistence.Error.t() | term()}
  def commit(%WorkspaceContext{} = context, %RunState{} = run, event, opts \\ [])
      when is_list(opts) do
    with :ok <- validate_opts(opts, [:command_id, :idempotency, :owner_id, :fencing_token]),
         true <- run.workspace_id == context.workspace_id,
         true <- run.event_seq > 1,
         {:ok, event} <- event_map(event) do
      Persistence.stores().runs.commit_transition(%CommitRunTransition{
        workspace_context: context,
        command_id: Keyword.get(opts, :command_id) || command_id("transition", run),
        expected_sequence: run.event_seq - 1,
        owner_id: Keyword.get(opts, :owner_id),
        fencing_token: Keyword.get(opts, :fencing_token),
        run: run,
        event: event,
        idempotency: Keyword.get(opts, :idempotency)
      })
    else
      false -> {:error, :invalid_run_transition}
      {:error, _reason} = error -> error
    end
  end

  @doc "Records one workspace-scoped cancellation request with atomic API idempotency."
  @spec request_cancellation(WorkspaceContext.t(), String.t(), map(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunCommitted.t()} | {:error, term()}
  def request_cancellation(%WorkspaceContext{} = context, run_id, reason, opts \\ [])
      when is_binary(run_id) and is_map(reason) and is_list(opts) do
    with :ok <- validate_opts(opts, [:command_id, :idempotency, :occurred_at]) do
      Persistence.stores().runs.request_cancellation(%RequestRunCancellation{
        workspace_context: context,
        command_id:
          Keyword.get(opts, :command_id) || bounded_command_id("cancel", {run_id, reason}),
        run_id: run_id,
        reason: reason,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
        idempotency: Keyword.get(opts, :idempotency)
      })
    end
  end

  @doc "Fetches one run without permitting cross-workspace identity probing."
  @spec get(WorkspaceContext.t(), String.t()) ::
          {:ok, RunState.t()} | {:error, FavnOrchestrator.Persistence.Error.t()}
  def get(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    with :ok <- validate_workspace_read(context) do
      Persistence.stores().runs.get_run(%GetRun{workspace_context: context, run_id: run_id})
    end
  end

  @doc "Returns one bounded keyset page of workspace runs."
  @spec page(WorkspaceContext.t(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.CursorPage.t(RunState.t())}
          | {:error, term()}
  def page(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with :ok <- validate_workspace_read(context),
         :ok <-
           validate_opts(opts, [
             :after,
             :manifest_version_id,
             :root_execution_group_id,
             :status,
             :limit
           ]) do
      Persistence.stores().runs.page_runs(%PageRuns{
        scope: context,
        after: Keyword.get(opts, :after),
        manifest_version_id: Keyword.get(opts, :manifest_version_id),
        root_execution_group_id: Keyword.get(opts, :root_execution_group_id),
        status: Keyword.get(opts, :status),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  @doc "Returns one bounded keyset page of compact relational run summaries."
  @spec page_summaries(WorkspaceContext.t(), keyword()) ::
          {:ok,
           FavnOrchestrator.Persistence.Results.CursorPage.t(
             FavnOrchestrator.Persistence.Results.RunSummary.t()
           )}
          | {:error, term()}
  def page_summaries(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    with :ok <- validate_workspace_read(context),
         :ok <-
           validate_opts(opts, [
             :after,
             :manifest_version_id,
             :root_execution_group_id,
             :status,
             :limit
           ]) do
      Persistence.stores().runs.page_run_summaries(%PageRuns{
        scope: context,
        after: Keyword.get(opts, :after),
        manifest_version_id: Keyword.get(opts, :manifest_version_id),
        root_execution_group_id: Keyword.get(opts, :root_execution_group_id),
        status: Keyword.get(opts, :status),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  @doc "Returns one event-id ordered page for all runs in an execution group."
  @spec page_group_events(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.CursorPage.t(map())} | {:error, term()}
  def page_group_events(%WorkspaceContext{} = context, root_run_id, opts \\ [])
      when is_binary(root_run_id) and is_list(opts) do
    with :ok <- validate_workspace_read(context),
         :ok <-
           validate_opts(opts, [:after_event_id, :before_event_id, :event_types, :limit, :order]) do
      Persistence.stores().runs.page_events(%PageRunEvents{
        workspace_context: context,
        root_execution_group_id: root_run_id,
        after_event_id: Keyword.get(opts, :after_event_id),
        before_event_id: Keyword.get(opts, :before_event_id),
        order: Keyword.get(opts, :order, :asc),
        event_types: Keyword.get(opts, :event_types),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  @doc "Returns one bounded sequence page from a run's canonical event stream."
  @spec page_events(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.CursorPage.t(map())} | {:error, term()}
  def page_events(%WorkspaceContext{} = context, run_id, opts \\ [])
      when is_binary(run_id) and is_list(opts) do
    with :ok <- validate_workspace_read(context),
         :ok <- validate_opts(opts, [:after_sequence, :event_types, :limit]) do
      Persistence.stores().runs.page_events(%PageRunEvents{
        workspace_context: context,
        run_id: run_id,
        after_sequence: Keyword.get(opts, :after_sequence),
        event_types: Keyword.get(opts, :event_types),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  @doc "Returns one bounded page ordered by a commit-safe outbox publication cursor."
  @spec page_published_events(WorkspaceContext.t() | Persistence.PlatformContext.t(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.CursorPage.t(map())} | {:error, term()}
  def page_published_events(scope, opts \\ []) when is_list(opts) do
    with :ok <- validate_opts(opts, [:after_publication_id, :event_types, :limit]) do
      Persistence.stores().runs.page_events(%PagePublishedRunEvents{
        scope: scope,
        after_publication_id: Keyword.get(opts, :after_publication_id),
        event_types: Keyword.get(opts, :event_types),
        limit: Keyword.get(opts, :limit, 50)
      })
    end
  end

  @doc "Persists one immutable, encrypted batch of runtime-input pins."
  @spec pin_runtime_inputs(WorkspaceContext.t(), String.t(), [Pin.t()], keyword()) ::
          {:ok, [Pin.t()]} | {:error, term()}
  def pin_runtime_inputs(%WorkspaceContext{} = context, run_id, pins, opts \\ [])
      when is_binary(run_id) and is_list(pins) and is_list(opts) do
    with :ok <- validate_opts(opts, [:command_id]) do
      Persistence.stores().runs.pin_runtime_inputs(%PinRuntimeInputs{
        workspace_context: context,
        command_id:
          Keyword.get(opts, :command_id) ||
            bounded_command_id("runtime-inputs", runtime_input_identity(run_id, pins)),
        run_id: run_id,
        pins: pins
      })
    end
  end

  @doc "Fetches a bounded set of decrypted runtime-input pins for a run."
  @spec get_runtime_inputs(WorkspaceContext.t(), String.t(), [Favn.Plan.node_key()] | nil) ::
          {:ok, [Pin.t()]} | {:error, term()}
  def get_runtime_inputs(%WorkspaceContext{} = context, run_id, node_keys \\ nil)
      when is_binary(run_id) and (is_list(node_keys) or is_nil(node_keys)) do
    with :ok <- validate_workspace_read(context) do
      Persistence.stores().runs.get_runtime_inputs(%GetRuntimeInputs{
        workspace_context: context,
        run_id: run_id,
        node_keys: node_keys
      })
    end
  end

  defp run_targets(%RunState{} = run, pipeline_refs) when is_list(pipeline_refs) do
    asset_targets =
      Enum.map(planned_asset_refs(run), fn {module, name} = ref ->
        %RunTarget{
          target_kind: :asset,
          target_id: TargetIdentity.for_asset(ref),
          target_module: Atom.to_string(module),
          target_name: Atom.to_string(name),
          is_primary: ref == run.asset_ref
        }
      end)

    pipeline_targets =
      Enum.map(pipeline_refs, fn {module, name} = ref ->
        %RunTarget{
          target_kind: :pipeline,
          target_id: TargetIdentity.for_pipeline(ref),
          target_module: Atom.to_string(module),
          target_name: Atom.to_string(name),
          is_primary: false
        }
      end)

    targets = asset_targets ++ pipeline_targets

    if targets != [] and
         length(targets) == length(Enum.uniq_by(targets, &{&1.target_kind, &1.target_id})) do
      {:ok, targets}
    else
      {:error, :invalid_run_targets}
    end
  rescue
    _error -> {:error, :invalid_run_targets}
  end

  defp planned_asset_refs(%RunState{target_refs: selected, plan: %Favn.Plan{nodes: nodes}})
       when is_list(selected) and is_map(nodes) do
    planned = Enum.map(Map.values(nodes), & &1.ref)

    selected
    |> Kernel.++(planned)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp planned_asset_refs(%RunState{target_refs: selected}) when is_list(selected),
    do: selected

  defp event_map(%RunEvent{} = event), do: {:ok, RunEvent.to_map(event)}
  defp event_map(event) when is_map(event), do: {:ok, event}
  defp event_map(_event), do: {:error, :invalid_run_event}

  defp validate_opts(opts, allowed) do
    if Keyword.keyword?(opts) do
      case Keyword.keys(opts) -- allowed do
        [] -> :ok
        unknown -> {:error, {:unknown_options, unknown}}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp validate_workspace_read(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
         ),
       do: :ok,
       else: {:error, Error.new(:forbidden, "workspace read role required")}
  end

  defp command_id(operation, run) do
    bounded_command_id(operation, "#{run.id}:#{run.event_seq}")
  end

  defp runtime_input_identity(run_id, pins) do
    pin_identity =
      pins
      |> Enum.map(&{&1.node_key, &1.payload_fingerprint})
      |> Enum.sort()
      |> :erlang.term_to_binary([:deterministic])

    run_id <> ":" <> Base.encode16(:crypto.hash(:sha256, pin_identity), case: :lower)
  end

  defp bounded_command_id(operation, identity) do
    digest = :crypto.hash(:sha256, identity) |> Base.encode16(case: :lower)
    operation <> ":" <> digest
  end
end

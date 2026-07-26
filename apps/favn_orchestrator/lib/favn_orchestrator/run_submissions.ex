defmodule FavnOrchestrator.RunSubmissions do
  @moduledoc """
  Durable asynchronous run-submission use cases.

  Producers resolve and freeze only the requested target and immutable
  deployment identity. Planning and run admission happen later in bounded
  submission workers.
  """

  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionByRunId
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmissionStats
  alias FavnOrchestrator.Persistence.Queries.PageRunSubmissions
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.Results.RunSubmissionPage
  alias FavnOrchestrator.Persistence.Results.RunSubmissionStats
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunSubmission.Intent
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnOrchestrator.Storage.JsonSafe

  @sources [:api, :operator, :scheduler, :backfill, :rebuild, :recovery, :child_run]
  @control_options [
    :_idempotency,
    :available_at,
    :deployment_id,
    :manifest_version_id,
    :occurred_at,
    :run_id,
    :submission_source
  ]

  @type source :: EnqueueRunSubmission.source()

  @doc "Loads one pending or terminal submission by its reserved run identity."
  @spec get(WorkspaceContext.t(), String.t()) ::
          {:ok, RunSubmission.t()} | {:error, term()}
  def get(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    Persistence.stores().run_submissions.get_by_run_id(%GetRunSubmissionByRunId{
      workspace_context: context,
      run_id: run_id
    })
  end

  @doc "Returns a bounded newest-first page of durable run submissions."
  @spec page(WorkspaceContext.t(), keyword()) ::
          {:ok, RunSubmissionPage.t()} | {:error, term()}
  def page(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    Persistence.stores().run_submissions.page(%PageRunSubmissions{
      workspace_context: context,
      status: Keyword.get(opts, :status),
      after: Keyword.get(opts, :after),
      limit: Keyword.get(opts, :limit, 100)
    })
  end

  @doc "Returns queue depth, age, retry, cancellation, and failure diagnostics."
  @spec stats(WorkspaceContext.t()) ::
          {:ok, RunSubmissionStats.t()} | {:error, term()}
  def stats(%WorkspaceContext{} = context) do
    Persistence.stores().run_submissions.stats(%GetRunSubmissionStats{
      workspace_context: context
    })
  end

  @spec enqueue_asset(WorkspaceContext.t(), Favn.Ref.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def enqueue_asset(%WorkspaceContext{} = context, {module, name} = asset_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    with {:ok, runtime, version} <- active_release(context, opts),
         target_id = ManifestTarget.asset_id(asset_ref),
         {:ok, _asset} <- ManifestTarget.resolve_asset(version, target_id) do
      enqueue_built(
        context,
        source(opts),
        runtime.deployment_id,
        version.manifest_version_id,
        "asset",
        target_id,
        :asset,
        target_id,
        opts
      )
    end
  end

  @spec enqueue_pipeline(WorkspaceContext.t(), {module(), atom()}, keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def enqueue_pipeline(%WorkspaceContext{} = context, {module, name} = pipeline_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    with {:ok, runtime, version} <- active_release(context, opts),
         target_id = ManifestTarget.pipeline_id(pipeline_ref),
         {:ok, _pipeline} <- ManifestTarget.resolve_pipeline(version, target_id) do
      enqueue_built(
        context,
        source(opts),
        runtime.deployment_id,
        version.manifest_version_id,
        "pipeline",
        target_id,
        :pipeline,
        target_id,
        opts
      )
    end
  end

  @spec enqueue_pipeline_assets(WorkspaceContext.t(), [Favn.Ref.t()], keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def enqueue_pipeline_assets(
        %WorkspaceContext{} = context,
        [{module, name} | _rest] = target_refs,
        opts
      )
      when is_atom(module) and is_atom(name) and is_list(opts) do
    with true <- Enum.all?(target_refs, &valid_ref?/1),
         {:ok, runtime, version} <- active_release(context, opts),
         :ok <- validate_assets(version, target_refs) do
      target_id = ManifestTarget.asset_id(hd(target_refs))

      enqueue_built(
        context,
        source(opts),
        runtime.deployment_id,
        version.manifest_version_id,
        "asset",
        target_id,
        :pipeline_assets,
        target_refs,
        opts
      )
    else
      false -> {:error, :invalid_run_submission_target}
      {:error, _reason} = error -> error
    end
  end

  @spec enqueue_rerun(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def enqueue_rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    with {:ok, %RunState{} = source_run} <- Runs.get(context, source_run_id),
         {:ok, target_kind, target_id} <- source_target(source_run),
         {:ok, version} <-
           ManifestStore.get_deployment_manifest(
             context,
             source_run.deployment_id,
             source_run.manifest_version_id
           ),
         :ok <- validate_target(version, target_kind, target_id) do
      enqueue_built(
        context,
        source(opts),
        source_run.deployment_id,
        source_run.manifest_version_id,
        target_kind,
        target_id,
        :rerun,
        source_run_id,
        opts
      )
    end
  end

  @doc "Requests cancellation before one reserved run identity is admitted."
  @spec cancel_pending(WorkspaceContext.t(), String.t(), map(), keyword()) ::
          :ok | {:error, term()}
  def cancel_pending(%WorkspaceContext{} = context, run_id, reason, opts \\ [])
      when is_binary(run_id) and is_map(reason) and is_list(opts) do
    store = Persistence.stores().run_submissions

    with {:ok, %RunSubmission{} = submission} <-
           store.get_by_run_id(%GetRunSubmissionByRunId{
             workspace_context: context,
             run_id: run_id
           }),
         :ok <- cancellable_submission?(submission),
         {:ok, reason_text} <- cancellation_reason(reason),
         occurred_at = Keyword.get(opts, :occurred_at, DateTime.utc_now()),
         command_id = cancellation_command_id(run_id, reason, opts),
         {:ok, %RunSubmission{}} <-
           store.request_cancellation(%RequestRunSubmissionCancellation{
             workspace_context: context,
             command_id: command_id,
             submission_id: submission.submission_id,
             reason: reason_text,
             occurred_at: occurred_at
           }) do
      :ok
    end
  end

  @doc false
  @spec build_target_command(
          WorkspaceContext.t(),
          source(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          :asset | :pipeline,
          term(),
          keyword()
        ) :: {:ok, EnqueueRunSubmission.t()} | {:error, term()}
  def build_target_command(
        %WorkspaceContext{} = context,
        source,
        deployment_id,
        manifest_version_id,
        target_kind,
        target_id,
        operation,
        selector,
        opts
      )
      when source in @sources and is_binary(deployment_id) and
             is_binary(manifest_version_id) and target_kind in ["asset", "pipeline"] and
             is_binary(target_id) and operation in [:asset, :pipeline] and is_list(opts) do
    with {:ok, version} <-
           ManifestStore.get_deployment_manifest(
             context,
             deployment_id,
             manifest_version_id
           ),
         :ok <- validate_target(version, target_kind, target_id),
         {:ok, intent} <- Intent.new(operation, selector, semantic_options(opts)) do
      {:ok,
       command(
         context,
         source,
         deployment_id,
         manifest_version_id,
         target_kind,
         target_id,
         intent,
         opts
       )}
    end
  end

  defp enqueue_built(
         context,
         source,
         deployment_id,
         manifest_version_id,
         target_kind,
         target_id,
         operation,
         selector,
         opts
       ) do
    with true <- source in @sources,
         {:ok, intent} <- Intent.new(operation, selector, semantic_options(opts)),
         command =
           command(
             context,
             source,
             deployment_id,
             manifest_version_id,
             target_kind,
             target_id,
             intent,
             opts
           ),
         {:ok, %RunSubmission{run_id: run_id}} <-
           Persistence.stores().run_submissions.enqueue(command) do
      {:ok, run_id}
    else
      false -> {:error, :invalid_run_submission_source}
      {:error, _reason} = error -> error
    end
  end

  defp command(
         context,
         source,
         deployment_id,
         manifest_version_id,
         target_kind,
         target_id,
         intent,
         opts
       ) do
    run_id = Keyword.get_lazy(opts, :run_id, fn -> new_id("run") end)
    idempotency_key = idempotency_key(Keyword.get(opts, :_idempotency), run_id)
    submission_id = new_id("run_submission")
    occurred_at = Keyword.get(opts, :occurred_at, DateTime.utc_now())

    request =
      {source, deployment_id, manifest_version_id, target_kind, target_id, run_id, intent}

    %EnqueueRunSubmission{
      workspace_context: context,
      command_id: "enqueue:#{submission_id}",
      submission_id: submission_id,
      source: source,
      idempotency_key: idempotency_key,
      request_hash: request_hash(request),
      deployment_id: deployment_id,
      manifest_version_id: manifest_version_id,
      target_kind: target_kind,
      target_id: target_id,
      run_id: run_id,
      intent: intent,
      occurred_at: occurred_at,
      available_at: Keyword.get(opts, :available_at)
    }
  end

  defp active_release(context, opts) do
    with {:ok, runtime} <- ManifestStore.get_runtime_state(context),
         requested = Keyword.get(opts, :manifest_version_id, runtime.manifest_version_id),
         true <- requested == runtime.manifest_version_id,
         {:ok, version} <-
           ManifestStore.get_deployment_manifest(
             context,
             runtime.deployment_id,
             runtime.manifest_version_id
           ) do
      {:ok, runtime, version}
    else
      false ->
        {:error, {:manifest_not_active_in_workspace, Keyword.get(opts, :manifest_version_id)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_target(version, "asset", target_id) do
    case ManifestTarget.resolve_asset(version, target_id) do
      {:ok, _asset} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_target(version, "pipeline", target_id) do
    case ManifestTarget.resolve_pipeline(version, target_id) do
      {:ok, _pipeline} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_assets(version, refs) do
    Enum.reduce_while(refs, :ok, fn ref, :ok ->
      case ManifestTarget.resolve_asset(version, ManifestTarget.asset_id(ref)) do
        {:ok, _asset} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp source_target(%RunState{} = run) do
    case pipeline_identity_ref(run.metadata) do
      {module, name} = ref when is_atom(module) and is_atom(name) ->
        {:ok, "pipeline", ManifestTarget.pipeline_id(ref)}

      nil ->
        case run.asset_ref do
          {module, name} = ref when is_atom(module) and is_atom(name) ->
            {:ok, "asset", ManifestTarget.asset_id(ref)}

          _invalid ->
            {:error, :invalid_rerun_source_target}
        end
    end
  end

  defp pipeline_identity_ref(metadata) when is_map(metadata) do
    Map.get(metadata, :pipeline_identity_ref) || Map.get(metadata, "pipeline_identity_ref")
  end

  defp pipeline_identity_ref(_metadata), do: nil

  defp semantic_options(opts), do: Keyword.drop(opts, @control_options)

  defp cancellable_submission?(%RunSubmission{status: status})
       when status in [:queued, :preparing, :cancelled],
       do: :ok

  defp cancellable_submission?(%RunSubmission{status: :submitted}),
    do: {:error, :run_already_submitted}

  defp cancellable_submission?(%RunSubmission{status: :admitting}),
    do: {:error, :run_admission_in_progress}

  defp cancellable_submission?(%RunSubmission{}), do: {:error, :run_already_terminal}

  defp cancellation_reason(reason) do
    with {:ok, encoded} <- Jason.encode(JsonSafe.data(reason)),
         true <- byte_size(encoded) in 1..2_048 do
      {:ok, encoded}
    else
      _invalid -> {:error, :invalid_run_submission_cancellation_reason}
    end
  end

  defp cancellation_command_id(run_id, reason, opts) do
    idempotency =
      case Keyword.get(opts, :idempotency) do
        %CommandIdempotency{} = value -> value.key_hash
        _missing -> request_hash({run_id, JsonSafe.data(reason)})
      end

    idempotency
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
    |> then(&"cancel-run-submission:#{&1}")
  end

  defp source(opts), do: Keyword.get(opts, :submission_source, :operator)

  defp idempotency_key(%CommandIdempotency{} = idempotency, _run_id) do
    request_hash({
      idempotency.operation,
      idempotency.principal_kind,
      idempotency.principal_id,
      idempotency.key_hash
    })
    |> Base.url_encode64(padding: false)
    |> then(&"external:#{&1}")
  end

  defp idempotency_key(nil, run_id) do
    run_id
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
    |> then(&"run:#{&1}")
  end

  defp request_hash(value) do
    {:ok, encoded} = PayloadCodec.encode(value)
    :crypto.hash(:sha256, encoded)
  end

  defp new_id(prefix) do
    suffix = 16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
    "#{prefix}_#{suffix}"
  end

  defp valid_ref?({module, name}), do: is_atom(module) and is_atom(name)
  defp valid_ref?(_ref), do: false
end

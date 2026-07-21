defmodule FavnOrchestrator.Backfills do
  @moduledoc """
  Resumable workspace-scoped backfill planning, dispatch, and progress queries.

  A submission first creates a deterministic terminal root run used only for
  lineage, then persists an immutable, batched window plan. Window execution is
  claimed and reconciled by `FavnOrchestrator.BackfillDispatcher`; API request
  processes never fan out thousands of child runs.
  """

  alias Favn.Backfill.RangeResolver
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Retry.Policy
  alias Favn.Window.Key
  alias FavnOrchestrator.Backfills.Submission
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Results.Backfill
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TransitionWriter

  @batch_size 500
  @default_max_windows 10_000

  @type plan :: %{
          required(:manifest_version_id) => String.t(),
          required(:deployment_id) => String.t(),
          required(:target_id) => String.t(),
          required(:kind) => atom(),
          required(:timezone) => String.t(),
          required(:window_count) => pos_integer(),
          required(:window_keys) => [String.t()],
          required(:range_start_at) => DateTime.t(),
          required(:range_end_at) => DateTime.t()
        }

  @doc "Resolves and validates a pipeline backfill without writing control-plane state."
  @spec plan_pipeline(WorkspaceContext.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, plan()} | {:error, term()}
  def plan_pipeline(context, manifest_version_id, target_id, range_request, opts \\ [])

  def plan_pipeline(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target_id,
        range_request,
        opts
      )
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with :ok <- authorize(context),
         :ok <- validate_options(opts),
         {:ok, runtime, version, pipeline} <-
           active_pipeline(context, manifest_version_id, target_id),
         {:ok, range} <- RangeResolver.resolve(range_request),
         :ok <- validate_window_count(range.requested_count, opts),
         {:ok, _resolution} <- resolve_pipeline(version, pipeline, List.first(range.anchors)) do
      {:ok, plan_map(runtime.deployment_id, version.manifest_version_id, target_id, range)}
    end
  end

  @doc "Persists and activates one deterministic, resumable pipeline backfill plan."
  @spec submit_pipeline(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          term(),
          keyword()
        ) :: {:ok, Backfill.t()} | {:error, term()}
  def submit_pipeline(context, manifest_version_id, target_id, range_request, opts \\ [])

  def submit_pipeline(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target_id,
        range_request,
        opts
      )
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with :ok <- authorize(context),
         :ok <- validate_options(opts),
         {:ok, runtime, version, pipeline} <-
           active_pipeline(context, manifest_version_id, target_id),
         {:ok, range} <- RangeResolver.resolve(range_request),
         :ok <- validate_window_count(range.requested_count, opts),
         {:ok, resolution} <- resolve_pipeline(version, pipeline, List.first(range.anchors)),
         submission <-
           build_submission(
             context,
             runtime,
             version,
             {:pipeline, pipeline, resolution},
             target_id,
             range,
             opts
           ),
         {:ok, _root} <- ensure_root_run(submission),
         {:ok, planning} <- start_plan(submission),
         {:ok, appended} <-
           append_batches(
             context,
             planning,
             submission.batches,
             submission.batch_hashes
           ) do
      activate(context, appended)
    end
  end

  @doc "Resolves and validates an asset backfill without writing control-plane state."
  @spec plan_asset(WorkspaceContext.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, plan()} | {:error, term()}
  def plan_asset(context, manifest_version_id, target_id, range_request, opts \\ [])

  def plan_asset(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target_id,
        range_request,
        opts
      )
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with :ok <- authorize(context),
         :ok <- validate_options(opts),
         {:ok, runtime, _version, _asset} <-
           active_asset(context, manifest_version_id, target_id),
         {:ok, range} <- RangeResolver.resolve(range_request),
         :ok <- validate_window_count(range.requested_count, opts) do
      {:ok, plan_map(runtime.deployment_id, manifest_version_id, target_id, range)}
    end
  end

  @doc "Persists and activates one deterministic, resumable asset backfill plan."
  @spec submit_asset(WorkspaceContext.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, Backfill.t()} | {:error, term()}
  def submit_asset(context, manifest_version_id, target_id, range_request, opts \\ [])

  def submit_asset(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target_id,
        range_request,
        opts
      )
      when is_binary(manifest_version_id) and is_binary(target_id) and is_list(opts) do
    with :ok <- authorize(context),
         :ok <- validate_options(opts),
         {:ok, runtime, version, asset} <- active_asset(context, manifest_version_id, target_id),
         {:ok, range} <- RangeResolver.resolve(range_request),
         :ok <- validate_window_count(range.requested_count, opts),
         submission <-
           build_submission(context, runtime, version, {:asset, asset}, target_id, range, opts),
         {:ok, _root} <- ensure_root_run(submission),
         {:ok, planning} <- start_plan(submission),
         {:ok, appended} <-
           append_batches(
             context,
             planning,
             submission.batches,
             submission.batch_hashes
           ) do
      activate(context, appended)
    end
  end

  @doc "Fetches one authoritative backfill under its workspace boundary."
  @spec get(WorkspaceContext.t(), String.t()) :: {:ok, Backfill.t()} | {:error, term()}
  def get(%WorkspaceContext{} = context, backfill_id) when is_binary(backfill_id) do
    store().get_backfill(%GetBackfill{workspace_context: context, backfill_id: backfill_id})
  end

  @doc "Returns one bounded keyset page of a backfill's windows."
  @spec page_windows(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page_windows(%WorkspaceContext{} = context, backfill_id, opts \\ [])
      when is_binary(backfill_id) and is_list(opts) do
    case Keyword.keys(opts) -- [:after, :limit, :status] do
      [] ->
        store().page_windows(%PageBackfillWindows{
          workspace_context: context,
          backfill_id: backfill_id,
          after: Keyword.get(opts, :after),
          limit: Keyword.get(opts, :limit, 100),
          status: Keyword.get(opts, :status)
        })

      unknown ->
        {:error, {:unknown_backfill_page_options, unknown}}
    end
  end

  defp active_pipeline(context, requested_manifest_id, target_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- runtime.manifest_version_id == requested_manifest_id,
         true <-
           Enum.any?(grants, &(&1.target_kind == :pipeline and &1.target_id == target_id)),
         {:ok, version} <- ManifestStore.get_manifest(context, requested_manifest_id),
         {:ok, %Pipeline{} = pipeline} <- ManifestTarget.resolve_pipeline(version, target_id) do
      {:ok, runtime, version, pipeline}
    else
      false -> {:error, :manifest_or_target_not_active_in_workspace}
      {:error, _reason} = error -> error
    end
  end

  defp active_asset(context, requested_manifest_id, target_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- runtime.manifest_version_id == requested_manifest_id,
         true <- Enum.any?(grants, &(&1.target_kind == :asset and &1.target_id == target_id)),
         {:ok, version} <- ManifestStore.get_manifest(context, requested_manifest_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      {:ok, runtime, version, asset}
    else
      false -> {:error, :manifest_or_target_not_active_in_workspace}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_pipeline(version, pipeline, first_anchor) do
    with {:ok, index} <- ManifestIndexCache.fetch(version) do
      PipelineResolver.resolve(index, pipeline,
        trigger: %{kind: :backfill, phase: :parent},
        params: %{},
        anchor_window: first_anchor
      )
    end
  end

  defp build_submission(context, runtime, version, target_spec, target_id, range, opts) do
    {target_kind, target, resolution} = submission_target(target_spec)
    root_run_id = Keyword.get(opts, :root_run_id) || random_id("run_backfill")
    backfill_id = Keyword.get(opts, :backfill_id) || deterministic_id("bf", root_run_id)
    batches = backfill_id |> plan_windows(range) |> Enum.chunk_every(@batch_size)

    %Submission{
      context: context,
      deployment_id: runtime.deployment_id,
      version: version,
      target_kind: target_kind,
      target: target,
      target_id: target_id,
      root_run_id: root_run_id,
      backfill_id: backfill_id,
      range: range,
      batches: batches,
      batch_hashes: Enum.map(batches, &BackfillPlan.batch_hash/1),
      resolution: resolution,
      opts: opts
    }
  end

  defp submission_target({:pipeline, pipeline, resolution}),
    do: {:pipeline, pipeline, resolution}

  defp submission_target({:asset, asset}), do: {:asset, asset, nil}

  defp ensure_root_run(%Submission{} = submission) do
    case Runs.get(submission.context, submission.root_run_id) do
      {:ok, %RunState{} = existing} ->
        validate_existing_root(existing, submission)

      {:error, %{kind: :not_found}} ->
        create_root_run(submission)

      {:error, _reason} = error ->
        error
    end
  end

  defp create_root_run(%Submission{} = submission) do
    {asset_ref, target_refs} = root_targets(submission)

    root =
      RunState.new(
        id: submission.root_run_id,
        workspace_id: submission.context.workspace_id,
        deployment_id: submission.deployment_id,
        manifest_version_id: submission.version.manifest_version_id,
        manifest_content_hash: submission.version.content_hash,
        required_runner_release_id: submission.version.required_runner_release_id,
        asset_ref: asset_ref,
        target_refs: target_refs,
        plan: nil,
        trigger: %{kind: :backfill, phase: :parent},
        metadata: root_metadata(submission),
        submit_kind: root_submit_kind(submission),
        root_run_id: submission.root_run_id
      )
      |> Map.put(:status, :ok)
      |> Map.put(:result, %{status: :accepted, backfill_id: submission.backfill_id})
      |> RunState.with_snapshot_hash()

    case persist_root_transition(submission, root) do
      {:ok, _replayed?} -> {:ok, root}
      {:error, _reason} = error -> error
    end
  end

  defp root_targets(%Submission{target_kind: :pipeline, resolution: resolution}),
    do: {List.first(resolution.target_refs), resolution.target_refs}

  defp root_targets(%Submission{target_kind: :asset, target: asset}),
    do: {asset.ref, [asset.ref]}

  defp root_metadata(%Submission{target_kind: :pipeline, target: pipeline} = submission) do
    %{
      backfill_id: submission.backfill_id,
      terminal_event_type: :run_finished,
      pipeline_identity_ref: {pipeline.module, pipeline.name},
      backfill_range: range_summary(submission.range),
      operator_metadata: Keyword.get(submission.opts, :metadata, %{})
    }
  end

  defp root_metadata(%Submission{target_kind: :asset, target: asset} = submission) do
    %{
      backfill_id: submission.backfill_id,
      terminal_event_type: :run_finished,
      asset_identity_ref: asset.ref,
      backfill_range: range_summary(submission.range),
      operator_metadata: Keyword.get(submission.opts, :metadata, %{})
    }
  end

  defp root_submit_kind(%Submission{target_kind: :pipeline}), do: :backfill_pipeline
  defp root_submit_kind(%Submission{target_kind: :asset}), do: :backfill_asset

  defp persist_root_transition(%Submission{} = submission, root) do
    options =
      [
        command_id: "backfill-root:create:#{submission.backfill_id}",
        return_commit?: true
      ]
      |> put_pipeline_refs(submission)

    TransitionWriter.persist_transition(
      submission.context,
      root,
      :backfill_started,
      %{
        status: :ok,
        backfill_id: submission.backfill_id,
        window_count: submission.range.requested_count
      },
      options
    )
  end

  defp put_pipeline_refs(options, %Submission{target_kind: :pipeline, target: pipeline}),
    do: Keyword.put(options, :pipeline_refs, [{pipeline.module, pipeline.name}])

  defp put_pipeline_refs(options, %Submission{target_kind: :asset}), do: options

  defp validate_existing_root(root, %Submission{} = submission) do
    linked_backfill =
      Map.get(root.metadata, :backfill_id) || Map.get(root.metadata, "backfill_id")

    if root.deployment_id == submission.deployment_id and
         root.manifest_version_id == submission.version.manifest_version_id and
         root.submit_kind == root_submit_kind(submission) and
         linked_backfill == submission.backfill_id do
      {:ok, root}
    else
      {:error, :backfill_root_conflict}
    end
  end

  defp start_plan(%Submission{} = submission) do
    store().start_plan(%StartBackfillPlan{
      workspace_context: submission.context,
      command_id: "backfill:start:#{submission.backfill_id}",
      backfill_id: submission.backfill_id,
      root_run_id: submission.root_run_id,
      deployment_id: submission.deployment_id,
      manifest_version_id: submission.version.manifest_version_id,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      range_start: submission.range.range_start_at,
      range_end: submission.range.range_end_at,
      expected_window_count: submission.range.requested_count,
      expected_batch_count: length(submission.batches),
      plan_hash: BackfillPlan.plan_hash(submission.batch_hashes),
      metadata: plan_execution_metadata(submission),
      occurred_at: DateTime.utc_now(),
      idempotency: Keyword.get(submission.opts, :idempotency)
    })
  end

  defp plan_execution_metadata(%Submission{target_kind: :pipeline} = submission),
    do: execution_metadata(submission.target, submission.opts)

  defp plan_execution_metadata(%Submission{target_kind: :asset} = submission),
    do: asset_execution_metadata(submission.target, submission.opts)

  defp append_batches(context, backfill, batches, hashes) do
    batches
    |> Enum.zip(hashes)
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, backfill}, fn {{windows, hash}, index}, {:ok, _current} ->
      command = %AppendBackfillPlanBatch{
        workspace_context: context,
        command_id: "backfill:batch:#{backfill.backfill_id}:#{index}",
        backfill_id: backfill.backfill_id,
        batch_index: index,
        batch_hash: hash,
        windows: windows,
        occurred_at: DateTime.utc_now()
      }

      case store().append_plan_batch(command) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp activate(context, backfill) do
    store().activate_plan(%ActivateBackfillPlan{
      workspace_context: context,
      command_id: "backfill:activate:#{backfill.backfill_id}",
      backfill_id: backfill.backfill_id,
      expected_version: backfill.version,
      occurred_at: DateTime.utc_now()
    })
  end

  defp plan_windows(backfill_id, range) do
    Enum.map(range.anchors, fn anchor ->
      key = Key.encode(anchor.key)

      %BackfillPlanWindow{
        window_id: deterministic_id("bfw", backfill_id <> <<0>> <> key),
        window_key: key,
        window_start: anchor.start_at,
        window_end: anchor.end_at,
        payload: %{
          "kind" => Atom.to_string(anchor.kind),
          "timezone" => anchor.timezone
        }
      }
    end)
  end

  defp execution_metadata(pipeline, opts) do
    %{
      "pipeline_module" => Atom.to_string(pipeline.module),
      "pipeline_name" => Atom.to_string(pipeline.name),
      "refresh" => encode_atom(Keyword.get(opts, :refresh)),
      "retry_policy" => encode_struct(Keyword.get(opts, :retry_policy)),
      "timeout_ms" => Keyword.get(opts, :timeout_ms),
      "operator_metadata" => Keyword.get(opts, :metadata, %{})
    }
  end

  defp asset_execution_metadata(asset, opts) do
    %{
      "asset_module" => asset.ref |> elem(0) |> Atom.to_string(),
      "asset_name" => asset.ref |> elem(1) |> Atom.to_string(),
      "dependencies" => encode_atom(Keyword.get(opts, :dependencies, :all)),
      "refresh" => encode_refresh(Keyword.get(opts, :refresh)),
      "retry_policy" => encode_struct(Keyword.get(opts, :retry_policy)),
      "timeout_ms" => Keyword.get(opts, :timeout_ms),
      "operator_metadata" => Keyword.get(opts, :metadata, %{})
    }
  end

  defp plan_map(deployment_id, manifest_version_id, target_id, range) do
    %{
      manifest_version_id: manifest_version_id,
      deployment_id: deployment_id,
      target_id: target_id,
      kind: range.kind,
      timezone: range.timezone,
      window_count: range.requested_count,
      window_keys: Enum.map(range.anchors, &Key.encode(&1.key)),
      range_start_at: range.range_start_at,
      range_end_at: range.range_end_at
    }
  end

  defp range_summary(range) do
    %{
      kind: range.kind,
      timezone: range.timezone,
      window_count: range.requested_count,
      range_start_at: range.range_start_at,
      range_end_at: range.range_end_at
    }
  end

  defp validate_window_count(count, opts) do
    max = Keyword.get(opts, :max_windows, @default_max_windows)

    cond do
      not (is_integer(max) and max > 0 and max <= @default_max_windows) ->
        {:error, :invalid_backfill_max_windows}

      count > max ->
        {:error, {:too_many_backfill_windows, count, max}}

      true ->
        :ok
    end
  end

  defp validate_options(opts) do
    if Keyword.keyword?(opts) do
      validate_keyword_options(opts)
    else
      {:error, :invalid_backfill_options}
    end
  end

  defp validate_keyword_options(opts) do
    with :ok <- validate_known_options(opts),
         :ok <- validate_optional_id(opts, :backfill_id, :invalid_backfill_id),
         :ok <-
           validate_optional_id(opts, :root_run_id, :invalid_backfill_root_run_id),
         :ok <- validate_idempotency(Keyword.get(opts, :idempotency)),
         :ok <- validate_metadata(Keyword.get(opts, :metadata, %{})),
         :ok <- validate_timeout(Keyword.get(opts, :timeout_ms)),
         :ok <- validate_dependencies(Keyword.get(opts, :dependencies, :all)) do
      validate_retry_policy(Keyword.get(opts, :retry_policy))
    end
  end

  defp validate_known_options(opts) do
    allowed = [
      :backfill_id,
      :root_run_id,
      :idempotency,
      :max_windows,
      :metadata,
      :dependencies,
      :refresh,
      :retry_policy,
      :timeout_ms
    ]

    case Keyword.keys(opts) -- allowed do
      [] -> :ok
      unknown -> {:error, {:unknown_backfill_options, unknown}}
    end
  end

  defp validate_optional_id(opts, key, error) do
    case Keyword.get(opts, key) do
      nil -> :ok
      value -> if valid_id?(value), do: :ok, else: {:error, error}
    end
  end

  defp validate_idempotency(nil), do: :ok
  defp validate_idempotency(%CommandIdempotency{}), do: :ok
  defp validate_idempotency(_value), do: {:error, :invalid_idempotency_context}

  defp validate_metadata(value) when is_map(value), do: :ok
  defp validate_metadata(_value), do: {:error, :invalid_backfill_metadata}

  defp validate_timeout(nil), do: :ok
  defp validate_timeout(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout(_value), do: {:error, :invalid_backfill_timeout}

  defp validate_dependencies(value) when value in [:all, :none], do: :ok
  defp validate_dependencies(_value), do: {:error, :invalid_backfill_dependencies}

  defp validate_retry_policy(nil), do: :ok

  defp validate_retry_policy(value) do
    case Policy.new(value) do
      {:ok, _policy} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp authorize(%WorkspaceContext{} = context) do
    if Enum.any?(
         context.roles,
         &(&1 in [:customer_operator, :workspace_admin, :platform_operator])
       ),
       do: :ok,
       else: {:error, :forbidden}
  end

  defp encode_atom(nil), do: nil
  defp encode_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_atom(value), do: value

  defp encode_refresh({:force_assets, refs}) when is_list(refs),
    do: %{
      "mode" => "force_assets",
      "refs" => Enum.map(refs, &encode_ref/1),
      "include_upstream" => false
    }

  defp encode_refresh({:force_assets, refs, opts}) when is_list(refs) and is_list(opts) do
    %{
      "mode" => "force_assets",
      "refs" => Enum.map(refs, &encode_ref/1),
      "include_upstream" => Keyword.get(opts, :include_upstream, false)
    }
  end

  defp encode_refresh(value), do: encode_atom(value)

  defp encode_ref({module, name}) when is_atom(module) and is_atom(name),
    do: %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}

  defp encode_struct(nil), do: nil
  defp encode_struct(%_{} = value), do: value |> Map.from_struct() |> encode_nested()
  defp encode_struct(value), do: encode_nested(value)

  defp encode_nested(%_{} = value), do: value |> Map.from_struct() |> encode_nested()

  defp encode_nested(value) when is_map(value),
    do: Map.new(value, fn {k, v} -> {k, encode_nested(v)} end)

  defp encode_nested(value) when is_list(value), do: Enum.map(value, &encode_nested/1)
  defp encode_nested(value), do: value

  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255

  defp random_id(prefix),
    do: prefix <> "_" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

  defp deterministic_id(prefix, value) do
    digest = :crypto.hash(:sha256, value) |> Base.url_encode64(padding: false)
    prefix <> "_" <> String.slice(digest, 0, 32)
  end

  defp store, do: Persistence.stores().backfills
end

defmodule FavnOrchestrator.TargetCompatibilityPlanner do
  @moduledoc """
  Inspects selected persisted targets and freezes manifest-deployment compatibility decisions.

  Decisions pin the observed binding version and active generation so PostgreSQL
  can reject a stale classification before switching the workspace deployment.
  """

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Asset
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Contract
  alias Favn.TargetCompatibility
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestInspectionAdmission
  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunnerTasks

  @binding_batch 500
  @inspection_concurrency 32
  @inspection_timeout_ms 300_000

  @doc false
  @spec default_inspection_timeout_ms() :: pos_integer()
  def default_inspection_timeout_ms, do: @inspection_timeout_ms

  @doc "Returns one frozen decision for every selected persisted SQL asset."
  @spec plan(
          PlatformContext.t(),
          WorkspaceContext.t(),
          Version.t(),
          DeploymentPlanner.t(),
          keyword()
        ) ::
          {:ok, [DeploymentTargetCompatibility.t()]} | {:error, term()}
  def plan(
        %PlatformContext{} = platform_context,
        %WorkspaceContext{} = workspace_context,
        %Version{} = version,
        %DeploymentPlanner{} = selection,
        opts \\ []
      ) do
    with {:ok, inspection_deadline_at} <- inspection_deadline_at(opts),
         {:ok, deployment_targets} <- DeploymentPlanner.plan(version, selection),
         {:ok, persisted} <- persisted_targets(version, deployment_targets),
         {:ok, bindings} <- fetch_bindings(workspace_context, persisted),
         {:ok, {active_versions, historical_descriptors}} <-
           active_versions(platform_context, bindings) do
      operation_id = Keyword.get(opts, :operation_id, version.manifest_version_id)

      progress = Keyword.get(opts, :progress)
      total = length(persisted)
      report_progress(progress, 0, total)

      persisted
      |> Task.async_stream(
        fn target ->
          ManifestInspectionAdmission.with_slot(fn ->
            classify_target(
              workspace_context,
              target,
              bindings,
              active_versions,
              historical_descriptors,
              version,
              operation_id,
              inspection_deadline_at
            )
          end)
        end,
        max_concurrency: @inspection_concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Enum.zip(persisted)
      |> Enum.with_index(1)
      |> collect_decisions(
        bindings,
        progress,
        total,
        workspace_context,
        version,
        operation_id,
        active_versions
      )
    end
  end

  defp collect_decisions(
         results,
         bindings,
         progress,
         total,
         workspace_context,
         version,
         operation_id,
         active_versions
       ) do
    results
    |> Enum.reduce({[], []}, fn {result, completed}, {decisions, errors} ->
      outcome =
        case result do
          {{:ok, {:planner_error, reason}}, _target} ->
            {:error, reason}

          {{:ok, decision}, _target} ->
            {:ok, decision}

          {{:exit, _reason}, target} ->
            case reconcile_exited_inspection(
                   workspace_context,
                   version,
                   operation_id,
                   target,
                   Map.get(bindings, target.target_id),
                   active_versions
                 ) do
              :ok ->
                {:ok,
                 inspection_unavailable_decision(
                   target,
                   Map.get(bindings, target.target_id)
                 )}

              {:error, reason} ->
                {:error, reason}
            end
        end

      if rem(completed, 10) == 0 or completed == total,
        do: report_progress(progress, completed, total)

      case outcome do
        {:ok, decision} -> {[decision | decisions], errors}
        {:error, reason} -> {decisions, [reason | errors]}
      end
    end)
    |> case do
      {decisions, []} -> {:ok, Enum.reverse(decisions)}
      {_decisions, errors} -> {:error, errors |> Enum.reverse() |> hd()}
    end
  end

  defp persisted_targets(version, deployment_targets) do
    deployment_targets
    |> Enum.filter(&(&1.target_kind == :asset))
    |> Enum.reduce_while({:ok, []}, fn target, {:ok, acc} ->
      case ManifestTarget.resolve_asset(version, target.target_id) do
        {:ok, %Asset{target_descriptor: %TargetDescriptor{}} = asset} ->
          {:cont, {:ok, [%{target_id: target.target_id, asset: asset} | acc]}}

        {:ok, %Asset{}} ->
          {:cont, {:ok, acc}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, targets} -> {:ok, Enum.sort_by(targets, & &1.target_id)}
      error -> error
    end)
  end

  defp fetch_bindings(_context, []), do: {:ok, %{}}

  defp fetch_bindings(context, targets) do
    targets
    |> Enum.map(& &1.target_id)
    |> Enum.chunk_every(@binding_batch)
    |> Enum.reduce_while({:ok, %{}}, fn target_ids, {:ok, acc} ->
      case Persistence.stores().target_generations.get_bindings(%GetTargetBindings{
             workspace_context: context,
             target_ids: target_ids
           }) do
        {:ok, bindings} ->
          {:cont, {:ok, Enum.reduce(bindings, acc, &Map.put(&2, &1.target_id, &1))}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp active_versions(platform_context, bindings) do
    with {:ok, {versions, historical_manifest_ids}} <-
           load_active_versions(platform_context, bindings),
         {:ok, historical_descriptors} <-
           historical_target_descriptors(
             platform_context,
             bindings,
             historical_manifest_ids
           ) do
      {:ok, {versions, historical_descriptors}}
    end
  end

  defp load_active_versions(platform_context, bindings) do
    worker = fn -> load_active_versions_bounded(platform_context, bindings) end
    with {:ok, result, _retained_bytes} <- ManifestMemory.manifest_worker(worker), do: result
  end

  defp load_active_versions_bounded(platform_context, bindings) do
    bindings
    |> Map.values()
    |> Enum.map(& &1.active_manifest_id)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, {%{}, MapSet.new()}}, fn
      manifest_id, {:ok, {versions, historical_manifest_ids}} ->
        case ManifestStore.get_manifest(platform_context, manifest_id) do
          {:ok, version} ->
            {:cont, {:ok, {Map.put(versions, manifest_id, version), historical_manifest_ids}}}

          {:error, %{details: %{reason: :historical_manifest_not_activatable}}} ->
            {:cont, {:ok, {versions, MapSet.put(historical_manifest_ids, manifest_id)}}}

          {:error, _reason} = error ->
            {:halt, error}
        end
    end)
  end

  defp historical_target_descriptors(platform_context, bindings, historical_manifest_ids) do
    bindings
    |> Map.values()
    |> Enum.filter(&MapSet.member?(historical_manifest_ids, &1.active_manifest_id))
    |> Enum.group_by(& &1.active_manifest_id, & &1.target_id)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, %{}}, fn {manifest_id, target_ids}, {:ok, acc} ->
      target_ids
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.chunk_every(@binding_batch)
      |> Enum.reduce_while({:ok, acc}, fn batch, {:ok, descriptors} ->
        case ManifestStore.get_manifest_target_descriptors(
               platform_context,
               manifest_id,
               batch
             ) do
          {:ok, fetched} ->
            descriptors =
              Enum.reduce(fetched, descriptors, fn descriptor, descriptors ->
                Map.put(descriptors, {manifest_id, descriptor.target_id}, descriptor)
              end)

            {:cont, {:ok, descriptors}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end)
      |> case do
        {:ok, descriptors} -> {:cont, {:ok, descriptors}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp classify_target(
         workspace_context,
         target,
         bindings,
         active_versions,
         historical_descriptors,
         desired_version,
         operation_id,
         inspection_deadline_at
       ) do
    binding = Map.get(bindings, target.target_id)

    active_target =
      active_target(
        binding,
        target,
        active_versions,
        historical_descriptors
      )

    active_descriptor = active_target && active_target.descriptor

    case inspection_target(active_target, binding, target.asset, desired_version) do
      {:ok, inspection_target, inspection_version} ->
        classify_inspected_target(
          workspace_context,
          target,
          binding,
          active_descriptor,
          inspection_target,
          inspection_version,
          operation_id,
          inspection_deadline_at
        )

      {:error, _reason} ->
        inspection_unavailable_decision(target, binding)
    end
  end

  defp classify_inspected_target(
         workspace_context,
         target,
         binding,
         active_descriptor,
         inspection_target,
         inspection_version,
         operation_id,
         inspection_deadline_at
       ) do
    case inspect_physical(
           workspace_context,
           inspection_target,
           inspection_version,
           inspection_asset_ref(inspection_target, target.asset.ref),
           target.target_id,
           operation_id,
           inspection_deadline_at
         ) do
      {:ok, observed} ->
        result =
          TargetCompatibility.classify(
            target.asset.target_descriptor,
            active_descriptor,
            binding && binding.active_physical_fingerprint,
            observed,
            target_contract(target.asset)
          )

        decision(target, binding, result.status, result.reason_code, result.diff)

      {:error, :inspection_runner_start_timeout} ->
        inspection_unavailable_decision(
          target,
          binding,
          :physical_inspection_runner_start_timeout,
          :timed_out
        )

      {:error, :runner_task_timeout} ->
        inspection_unavailable_decision(
          target,
          binding,
          :physical_inspection_timeout,
          :timed_out
        )

      {:error, {:inspection_timeout_reconciliation_failed, _details} = reason} ->
        {:planner_error, reason}

      {:error, _reason} ->
        inspection_unavailable_decision(target, binding)
    end
  end

  defp inspection_target(
         %{inspection: :asset, version: active_version} = active_target,
         binding,
         _desired_asset,
         desired_version
       ) do
    if active_version.runner_releases == desired_version.runner_releases do
      {:ok, {:asset, active_target.asset}, active_version}
    else
      with {:ok, relation} <- active_physical_relation(binding, active_target.connection) do
        {:ok, {:relation, relation}, desired_version}
      end
    end
  end

  defp inspection_target(
         %{inspection: :persisted_relation, connection: connection},
         binding,
         _desired_asset,
         desired_version
       ) do
    with {:ok, relation} <- active_physical_relation(binding, connection) do
      {:ok, {:relation, relation}, desired_version}
    end
  end

  defp inspection_target(nil, _binding, desired_asset, desired_version),
    do: {:ok, {:asset, desired_asset}, desired_version}

  defp inspection_asset_ref({:asset, %Asset{ref: asset_ref}}, _fallback), do: asset_ref
  defp inspection_asset_ref({:relation, %RelationRef{}}, fallback), do: fallback

  defp target_contract(%Asset{assurance: %{contract: %Contract{} = contract}}), do: contract
  defp target_contract(%Asset{}), do: nil

  defp active_physical_relation(%{active_physical_relation: relation}, connection)
       when is_map(relation) do
    if persisted_connection(relation) in persisted_connection_values(connection) do
      relation =
        relation
        |> Map.drop([:connection, "connection"])
        |> Map.put(:connection, connection)
        |> RelationRef.new!()

      {:ok, relation}
    else
      {:error, :active_physical_relation_connection_changed}
    end
  rescue
    ArgumentError -> {:error, :invalid_active_physical_relation}
  end

  defp active_physical_relation(_binding, _connection),
    do: {:error, :active_physical_relation_missing}

  defp persisted_connection(relation),
    do: Map.get(relation, :connection, Map.get(relation, "connection"))

  defp persisted_connection_values(nil), do: [nil]

  defp persisted_connection_values(connection) when is_atom(connection),
    do: [connection, Atom.to_string(connection)]

  defp active_target(nil, _target, _versions, _historical_descriptors), do: nil

  defp active_target(
         %{active_generation_id: nil},
         _target,
         _versions,
         _historical_descriptors
       ),
       do: nil

  defp active_target(binding, target, versions, historical_descriptors) do
    with manifest_id when is_binary(manifest_id) <- binding.active_manifest_id,
         %Version{} = version <- Map.get(versions, manifest_id),
         {:ok, %Asset{target_descriptor: %TargetDescriptor{} = descriptor} = asset} <-
           ManifestTarget.resolve_asset(version, target.target_id),
         true <- descriptor.descriptor_hash == binding.active_descriptor_hash do
      %{
        descriptor: descriptor,
        asset: asset,
        connection: asset.relation.connection,
        inspection: :asset,
        version: version
      }
    else
      _missing_or_mismatched ->
        historical_target(binding, target, historical_descriptors)
    end
  end

  defp historical_target(
         %{active_manifest_id: manifest_id, active_descriptor_hash: descriptor_hash},
         %{target_id: target_id, asset: %Asset{} = asset},
         historical_descriptors
       ) do
    case Map.get(historical_descriptors, {manifest_id, target_id}) do
      %TargetDescriptor{descriptor_hash: ^descriptor_hash} = descriptor ->
        %{
          descriptor: descriptor,
          connection: asset.relation.connection,
          inspection: :persisted_relation
        }

      _missing_or_mismatched ->
        nil
    end
  end

  defp inspect_physical(
         workspace_context,
         target,
         version,
         asset_ref,
         target_id,
         operation_id,
         inspection_deadline_at
       ) do
    {:ok, binding} = OperationRunnerTasks.binding(version, asset_ref)
    request = inspection_request(target, version, binding.required_runner_release_id)

    with {:ok, task} <-
           ensure_inspection_task(
             workspace_context,
             version,
             asset_ref,
             request,
             operation_id,
             target_id,
             inspection_deadline_at
           ),
         {:ok, %RelationInspectionResult{} = result} <-
           await_inspection(workspace_context, task, inspection_deadline_at),
         :ok <-
           RunnerIdentityVerifier.verify_inspection_result(
             binding.required_runner_release_id,
             result
           ),
         {:ok, physical} <- PhysicalFingerprint.from_inspection(result) do
      {:ok, physical}
    else
      {:ok, _invalid} -> {:error, :invalid_runner_inspection_result}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_inspection_task(
         context,
         version,
         asset_ref,
         request,
         operation_id,
         target_id,
         deadline_at
       ) do
    domain_identity = {:deployment_target_inspection, operation_id, target_id}

    task_id =
      OperationRunnerTasks.task_id(
        context.workspace_id,
        :relation_inspection,
        domain_identity,
        version
      )

    case OperationRunnerTasks.fetch(context, task_id) do
      {:ok, _existing} ->
        OperationRunnerTasks.ensure(
          context,
          version,
          asset_ref,
          :relation_inspection,
          request,
          domain_identity,
          deadline_at: deadline_at
        )

      {:error, %PersistenceError{kind: :not_found}} ->
        with :ok <- inspection_deadline_available(deadline_at) do
          OperationRunnerTasks.ensure(
            context,
            version,
            asset_ref,
            :relation_inspection,
            request,
            domain_identity,
            deadline_at: deadline_at
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp inspection_deadline_available(deadline_at) do
    if DateTime.compare(deadline_at, DateTime.utc_now()) == :lt,
      do: {:error, :inspection_runner_start_timeout},
      else: :ok
  end

  defp await_inspection(context, task, fallback_deadline_at) do
    deadline_at = task.deadline_at || fallback_deadline_at

    remaining_ms =
      deadline_at
      |> DateTime.diff(DateTime.utc_now(), :millisecond)
      |> max(0)

    result =
      cond do
        task.status in [:succeeded, :failed, :cancelled, :unknown] ->
          OperationRunnerTasks.await(context, task.task_id, timeout: 1)

        remaining_ms == 0 ->
          {:error, :runner_task_timeout}

        true ->
          OperationRunnerTasks.await(context, task.task_id, timeout: remaining_ms)
      end

    case result do
      {:error, :runner_task_timeout} -> reconcile_inspection_timeout(context, task.task_id)
      other -> other
    end
  end

  defp reconcile_inspection_timeout(context, task_id) do
    with {:ok, task} <- OperationRunnerTasks.fetch(context, task_id) do
      timeout_reason =
        if task.status == :queued,
          do: :inspection_runner_start_timeout,
          else: :runner_task_timeout

      case cancel_and_confirm(context, task, :deadline_reached) do
        :ok ->
          {:error, timeout_reason}

        {:error, details} ->
          {:error, {:inspection_timeout_reconciliation_failed, details}}
      end
    else
      {:error, reason} ->
        {:error,
         {:inspection_timeout_reconciliation_failed, %{task_id: task_id, fetch_error: reason}}}
    end
  end

  defp reconcile_exited_inspection(
         context,
         desired_version,
         operation_id,
         target,
         binding,
         active_versions
       ) do
    [desired_version, binding && Map.get(active_versions, binding.active_manifest_id)]
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq_by(&{&1.manifest_version_id, &1.content_hash})
    |> Enum.reduce_while(:ok, fn version, :ok ->
      task_id =
        OperationRunnerTasks.task_id(
          context.workspace_id,
          :relation_inspection,
          {:deployment_target_inspection, operation_id, target.target_id},
          version
        )

      case reconcile_exited_task(context, task_id) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp reconcile_exited_task(context, task_id) do
    case OperationRunnerTasks.fetch(context, task_id) do
      {:ok, %{status: status}} when status in [:succeeded, :failed, :cancelled, :unknown] ->
        :ok

      {:ok, task} ->
        case cancel_and_confirm(context, task, :inspection_classifier_stopped) do
          :ok -> :ok
          {:error, details} -> {:error, {:inspection_timeout_reconciliation_failed, details}}
        end

      {:error, %PersistenceError{kind: :not_found}} ->
        :ok

      {:error, reason} ->
        {:error,
         {:inspection_timeout_reconciliation_failed, %{task_id: task_id, fetch_error: reason}}}
    end
  end

  defp cancel_and_confirm(context, task, reason) do
    cancellation = RunnerTasks.request_cancellation(context.workspace_id, task.task_id, reason)

    case OperationRunnerTasks.fetch(context, task.task_id) do
      {:ok, %{status: status}} when status in [:succeeded, :failed, :cancelled, :unknown] ->
        :ok

      {:ok, pending} ->
        {:error,
         %{
           task_id: task.task_id,
           task_status: pending.status,
           cancellation_status: cancellation.status
         }}

      {:error, fetch_error} ->
        {:error,
         %{
           task_id: task.task_id,
           cancellation_status: cancellation.status,
           fetch_error: fetch_error
         }}
    end
  end

  defp inspection_timeout_ms(opts) do
    case Keyword.get(opts, :inspection_timeout_ms, default_inspection_timeout_ms()) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> {:ok, timeout_ms}
      _invalid -> {:error, :invalid_manifest_inspection_timeout}
    end
  end

  defp inspection_deadline_at(opts) do
    case Keyword.get(opts, :inspection_deadline_at) do
      nil ->
        with {:ok, timeout_ms} <- inspection_timeout_ms(opts) do
          {:ok, DateTime.add(DateTime.utc_now(), timeout_ms, :millisecond)}
        end

      %DateTime{} = deadline_at ->
        {:ok, deadline_at}

      _invalid ->
        {:error, :invalid_manifest_inspection_deadline}
    end
  end

  defp inspection_request({:asset, asset}, version, release_id) do
    %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: release_id,
      asset_ref: asset.ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }
  end

  defp inspection_request({:relation, %RelationRef{} = relation}, version, release_id) do
    %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: release_id,
      relation: relation,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }
  end

  defp inspection_unavailable_decision(target, binding) do
    inspection_unavailable_decision(
      target,
      binding,
      :physical_inspection_unavailable,
      :unavailable
    )
  end

  defp inspection_unavailable_decision(target, binding, reason_code, status) do
    decision(
      target,
      binding,
      :operator_decision,
      reason_code,
      %{inspection: %{status: status}}
    )
  end

  defp decision(target, binding, status, reason_code, diff) do
    %DeploymentTargetCompatibility{
      target_id: target.target_id,
      desired_descriptor_hash: target.asset.target_descriptor.descriptor_hash,
      compatibility_status: status,
      reason_code: Atom.to_string(reason_code),
      compatibility_diff: diff,
      expected_binding_version: binding && binding.version,
      expected_active_generation_id: binding && binding.active_generation_id,
      active_physical_fingerprint: binding && binding.active_physical_fingerprint
    }
  end

  defp report_progress(progress, completed, total) when is_function(progress, 2) do
    _ = progress.(completed, total)
    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp report_progress(_progress, _completed, _total), do: :ok
end

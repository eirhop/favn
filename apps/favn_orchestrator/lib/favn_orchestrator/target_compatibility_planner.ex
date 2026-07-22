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
  alias Favn.TargetCompatibility
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.RunnerManifestRegistration
  alias FavnOrchestrator.RunnerReleaseCompatibility
  alias FavnOrchestrator.RuntimeConfig

  @binding_batch 500
  @max_concurrency 8

  @doc "Returns one frozen decision for every selected persisted SQL asset."
  @spec plan(PlatformContext.t(), WorkspaceContext.t(), Version.t(), DeploymentPlanner.t()) ::
          {:ok, [DeploymentTargetCompatibility.t()]} | {:error, term()}
  def plan(
        %PlatformContext{} = platform_context,
        %WorkspaceContext{} = workspace_context,
        %Version{} = version,
        %DeploymentPlanner{} = selection
      ) do
    with {:ok, deployment_targets} <- DeploymentPlanner.plan(version, selection),
         {:ok, persisted} <- persisted_targets(version, deployment_targets),
         {:ok, bindings} <- fetch_bindings(workspace_context, persisted),
         {:ok, active_versions} <- active_versions(platform_context, bindings),
         :ok <- ensure_inspection_versions(active_versions, version) do
      persisted
      |> Task.async_stream(
        fn target ->
          classify_target(target, bindings, active_versions, version)
        end,
        max_concurrency: min(System.schedulers_online(), @max_concurrency),
        ordered: true,
        timeout: 30_000
      )
      |> Enum.zip(persisted)
      |> Enum.reduce_while({:ok, []}, &collect_decision/2)
      |> then(fn
        {:ok, decisions} -> {:ok, Enum.reverse(decisions)}
        {:error, _reason} = error -> error
      end)
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
    bindings
    |> Map.values()
    |> Enum.map(& &1.active_manifest_id)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn manifest_id, {:ok, acc} ->
      case ManifestStore.get_manifest(platform_context, manifest_id) do
        {:ok, version} -> {:cont, {:ok, Map.put(acc, manifest_id, version)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ensure_inspection_versions(active_versions, desired_version) do
    runtime = RuntimeConfig.current()

    active_versions
    |> Map.values()
    |> Enum.sort_by(& &1.manifest_version_id)
    |> Enum.reduce_while(:ok, fn version, :ok ->
      if version.required_runner_release_id == desired_version.required_runner_release_id do
        case RunnerManifestRegistration.ensure(
               runtime.runner_client,
               version,
               runtime.runner_client_opts
             ) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      else
        {:halt,
         {:error,
          {:runner_release_mismatch, version.required_runner_release_id,
           desired_version.required_runner_release_id}}}
      end
    end)
  end

  defp classify_target(target, bindings, active_versions, desired_version) do
    binding = Map.get(bindings, target.target_id)
    active_target = active_target(binding, target.target_id, active_versions)
    active_descriptor = active_target && active_target.descriptor
    inspection_asset = (active_target && active_target.asset) || target.asset
    inspection_version = (active_target && active_target.version) || desired_version

    case inspect_physical(inspection_asset, inspection_version) do
      {:ok, observed} ->
        result =
          TargetCompatibility.classify(
            target.asset.target_descriptor,
            active_descriptor,
            binding && binding.active_physical_fingerprint,
            observed
          )

        decision(target, binding, result.status, result.reason_code, result.diff)

      {:error, _reason} ->
        decision(
          target,
          binding,
          :operator_decision,
          :physical_inspection_unavailable,
          %{inspection: %{status: :unavailable}}
        )
    end
  end

  defp active_target(nil, _target_id, _versions), do: nil

  defp active_target(%{active_generation_id: nil}, _target_id, _versions), do: nil

  defp active_target(binding, target_id, versions) do
    with manifest_id when is_binary(manifest_id) <- binding.active_manifest_id,
         %Version{} = version <- Map.get(versions, manifest_id),
         {:ok, %Asset{target_descriptor: %TargetDescriptor{} = descriptor} = asset} <-
           ManifestTarget.resolve_asset(version, target_id),
         true <- descriptor.descriptor_hash == binding.active_descriptor_hash do
      %{descriptor: descriptor, asset: asset, version: version}
    else
      _missing_or_mismatched -> nil
    end
  end

  defp inspect_physical(asset, version) do
    runtime = RuntimeConfig.current()

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      asset_ref: asset.ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    with {:ok, %RelationInspectionResult{} = result} <-
           RunnerDispatch.inspect_relation(
             runtime.runner_client,
             request,
             runtime.runner_client_opts
           ),
         :ok <-
           RunnerReleaseCompatibility.verify_inspection_result(
             version.required_runner_release_id,
             result
           ),
         {:ok, physical} <- PhysicalFingerprint.from_inspection(result) do
      {:ok, physical}
    else
      {:ok, _invalid} -> {:error, :invalid_runner_inspection_result}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_runner_inspection_result}
    end
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

  defp collect_decision(
         {{:ok, %DeploymentTargetCompatibility{} = decision}, _target},
         {:ok, acc}
       ),
       do: {:cont, {:ok, [decision | acc]}}

  defp collect_decision({{:ok, {:error, reason}}, target}, _acc),
    do: {:halt, {:error, {:target_compatibility_failed, target.target_id, reason}}}

  defp collect_decision({{:exit, reason}, target}, _acc),
    do: {:halt, {:error, {:target_compatibility_failed, target.target_id, reason}}}
end

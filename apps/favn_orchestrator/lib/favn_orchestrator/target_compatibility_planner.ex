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
  @inspection_batch 500
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
         {:ok, {active_versions, historical_descriptors}} <-
           active_versions(platform_context, bindings),
         :ok <- ensure_inspection_versions(active_versions, version) do
      inputs =
        Enum.map(persisted, fn target ->
          classification_input(
            target,
            bindings,
            active_versions,
            historical_descriptors,
            version
          )
        end)

      {:ok, classify_targets(inputs)}
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
        {:cont, :ok}
      end
    end)
  end

  defp classification_input(
         target,
         bindings,
         active_versions,
         historical_descriptors,
         desired_version
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
        {:inspect,
         %{
           target: target,
           binding: binding,
           active_descriptor: active_descriptor,
           inspection_target: inspection_target,
           inspection_version: inspection_version
         }}

      {:error, _reason} ->
        {:decision, inspection_unavailable_decision(target, binding)}
    end
  end

  defp classify_targets(inputs) do
    inspections =
      inputs
      |> Enum.flat_map(fn
        {:inspect, input} -> [input]
        {:decision, _decision} -> []
      end)
      |> inspect_physical_batch()

    {decisions, []} =
      Enum.map_reduce(inputs, inspections, fn
        {:decision, decision}, inspections ->
          {decision, inspections}

        {:inspect, input}, [inspection | inspections] ->
          {classify_inspected_target(input, inspection), inspections}
      end)

    decisions
  end

  defp classify_inspected_target(
         %{
           target: target,
           binding: binding,
           active_descriptor: active_descriptor
         },
         inspection
       ) do
    case inspection do
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
    if active_version.required_runner_release_id == desired_version.required_runner_release_id do
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

  defp inspect_physical_batch([]), do: []

  defp inspect_physical_batch(inputs) do
    inputs
    |> Enum.chunk_every(@inspection_batch)
    |> Enum.flat_map(&inspect_physical_chunk/1)
  end

  defp inspect_physical_chunk(inputs) do
    runtime = RuntimeConfig.current()

    requests =
      Enum.map(inputs, fn input ->
        inspection_request(input.inspection_target, input.inspection_version)
      end)

    case RunnerDispatch.inspect_relations(
           runtime.runner_client,
           requests,
           runtime.runner_client_opts
         ) do
      {:ok, results} when length(results) == length(inputs) ->
        inputs
        |> Enum.zip(results)
        |> Enum.map(fn {input, result} ->
          normalize_inspection_result(result, input.inspection_version)
        end)

      {:ok, _invalid} ->
        List.duplicate({:error, :invalid_runner_inspection_batch}, length(inputs))

      {:error, reason} ->
        List.duplicate({:error, reason}, length(inputs))
    end
  end

  defp normalize_inspection_result({:ok, %RelationInspectionResult{} = result}, version) do
    with :ok <-
           RunnerReleaseCompatibility.verify_inspection_result(
             version.required_runner_release_id,
             result
           ),
         {:ok, physical} <- PhysicalFingerprint.from_inspection(result) do
      {:ok, physical}
    end
  end

  defp normalize_inspection_result({:error, _reason} = error, _version), do: error

  defp normalize_inspection_result(_invalid, _version),
    do: {:error, :invalid_runner_inspection_result}

  defp inspection_request({:asset, asset}, version) do
    %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      asset_ref: asset.ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }
  end

  defp inspection_request({:relation, %RelationRef{} = relation}, version) do
    %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id,
      relation: relation,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }
  end

  defp inspection_unavailable_decision(target, binding) do
    decision(
      target,
      binding,
      :operator_decision,
      :physical_inspection_unavailable,
      %{inspection: %{status: :unavailable}}
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
end

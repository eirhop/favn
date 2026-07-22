defmodule FavnStoragePostgres.TargetGenerations.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.TargetGenerationStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.GenerationDataPlaneMarker
  alias Favn.Manifest.TargetDescriptor
  alias Favn.TargetGeneration
  alias FavnOrchestrator.Persistence.Commands.EnsureWritableTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.ReconcileInitialTargetGeneration
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetTargetBinding
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Results.InitialTargetGenerationReconciliation
  alias FavnOrchestrator.Persistence.Results.WritableTargetGeneration
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AssetTargetBinding
  alias FavnStoragePostgres.Schemas.AssetTargetGeneration
  alias FavnStoragePostgres.Schemas.Materialization
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment

  @max_batch 500
  @writable_statuses ~w(ready uninitialized rebuild_available)

  @impl true
  def ensure_writable(%EnsureWritableTargetGeneration{} = command) do
    with :ok <- validate_ensure(command),
         {:ok, result} <- transaction(fn -> ensure_writable!(command) end) do
      {:ok, result}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def reconcile_initial(%ReconcileInitialTargetGeneration{} = command) do
    with :ok <- validate_reconcile_initial(command),
         {:ok, result} <- transaction(fn -> reconcile_initial!(command) end) do
      {:ok, result}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_binding(%GetTargetBinding{} = query) do
    with :ok <- validate_get(query.workspace_context, [query.target_id]) do
      binding =
        from(binding in AssetTargetBinding,
          left_join: generation in AssetTargetGeneration,
          on:
            generation.workspace_id == binding.workspace_id and
              generation.target_id == binding.target_id and
              generation.target_generation_id == binding.active_generation_id,
          where:
            binding.workspace_id == ^query.workspace_context.workspace_id and
              binding.target_id == ^query.target_id,
          select: {binding, generation}
        )
        |> Repo.one()

      {:ok, if(binding, do: binding_result(binding), else: nil)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_bindings(%GetTargetBindings{} = query) do
    with :ok <- validate_get(query.workspace_context, query.target_ids) do
      bindings =
        from(binding in AssetTargetBinding,
          left_join: generation in AssetTargetGeneration,
          on:
            generation.workspace_id == binding.workspace_id and
              generation.target_id == binding.target_id and
              generation.target_generation_id == binding.active_generation_id,
          where:
            binding.workspace_id == ^query.workspace_context.workspace_id and
              binding.target_id in ^query.target_ids,
          order_by: [asc: binding.target_id],
          select: {binding, generation}
        )
        |> Repo.all()
        |> Enum.map(&binding_result/1)

      {:ok, bindings}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp ensure_writable!(command) do
    workspace_id = command.workspace_context.workspace_id
    descriptor = command.descriptor
    lock_target_identity!(workspace_id, command.target_id)

    case lock_binding(workspace_id, command.target_id) do
      nil -> initialize_target!(command)
      binding -> resolve_binding!(binding, command, descriptor)
    end
  end

  defp reconcile_initial!(command) do
    workspace_id = command.workspace_context.workspace_id
    lock_target_identity!(workspace_id, command.target_id)

    binding = lock_binding(workspace_id, command.target_id)
    generation = lock_generation!(workspace_id, command.target_id, command.target_generation_id)
    materialization = successful_materialization!(command)

    cond do
      exact_initial_reconciliation_replay?(binding, generation, command) ->
        initial_reconciliation_result(generation, binding, materialization.materialization_id)

      is_nil(binding) ->
        Repo.rollback(Error.new(:not_found, "target binding not found"))

      binding.desired_manifest_id != command.manifest_version_id ->
        Repo.rollback(Error.new(:conflict, "target binding is stale for the selected manifest"))

      binding.compatibility_status != "uninitialized" or
          not is_nil(binding.active_generation_id) ->
        Repo.rollback(
          Error.new(:conflict, "target is not awaiting initial generation activation")
        )

      generation.status != "building" or
          not is_nil(generation.creating_rebuild_operation_id) ->
        Repo.rollback(
          Error.new(:conflict, "target generation is not an initial building generation")
        )

      generation.creating_descriptor_hash != binding.desired_descriptor_hash ->
        Repo.rollback(Error.new(:conflict, "target generation descriptor is stale"))

      true ->
        activate_initial_generation!(generation, binding, materialization, command)
    end
  end

  defp successful_materialization!(command) do
    materialization =
      from(materialization in Materialization,
        where:
          materialization.workspace_id == ^command.workspace_context.workspace_id and
            materialization.materialization_id == ^command.materialization_id,
        lock: "FOR KEY SHARE"
      )
      |> Repo.one()

    deployment_manifest_id =
      case materialization do
        %Materialization{} ->
          from(deployment in WorkspaceDeployment,
            where:
              deployment.workspace_id == ^materialization.workspace_id and
                deployment.deployment_id == ^materialization.deployment_id,
            select: deployment.manifest_version_id
          )
          |> Repo.one()

        nil ->
          nil
      end

    if match?(%Materialization{}, materialization) and
         materialization.target_kind == "asset" and
         materialization.target_id == command.target_id and
         materialization.target_generation_id == command.target_generation_id and
         materialization.evidence_generation_id == command.target_generation_id and
         deployment_manifest_id == command.manifest_version_id do
      materialization
    else
      Repo.rollback(
        Error.new(:conflict, "successful materialization evidence does not match generation")
      )
    end
  end

  defp exact_initial_reconciliation_replay?(nil, _generation, _command), do: false

  defp exact_initial_reconciliation_replay?(binding, generation, command) do
    binding.active_generation_id == command.target_generation_id and
      binding.desired_manifest_id == command.manifest_version_id and
      binding.compatibility_status == "ready" and generation.status == "active" and
      generation.activation_token == command.command_id and
      generation.active_descriptor_hash == binding.desired_descriptor_hash and
      generation.physical_schema_fingerprint == command.physical_schema_fingerprint and
      generation.data_plane_marker == canonical_json_map(command.data_plane_marker) and
      binding.active_physical_fingerprint == command.physical_schema_fingerprint
  end

  defp activate_initial_generation!(generation, binding, materialization, command) do
    activated_generation =
      generation
      |> Ecto.Changeset.change(%{
        active_descriptor_hash: binding.desired_descriptor_hash,
        physical_schema_fingerprint: command.physical_schema_fingerprint,
        data_plane_marker: canonical_json_map(command.data_plane_marker),
        activation_token: command.command_id,
        status: "active",
        version: generation.version + 1,
        activated_at: command.occurred_at,
        updated_at: command.occurred_at
      })
      |> Repo.update!()

    activated_binding =
      binding
      |> Ecto.Changeset.change(%{
        active_generation_id: generation.target_generation_id,
        compatibility_status: "ready",
        reason_code: "initial_materialization_reconciled",
        compatibility_diff: %{},
        active_physical_fingerprint: command.physical_schema_fingerprint,
        version: binding.version + 1,
        updated_at: command.occurred_at
      })
      |> Repo.update!()

    initial_reconciliation_result(
      activated_generation,
      activated_binding,
      materialization.materialization_id
    )
  end

  defp initialize_target!(command) do
    descriptor = command.descriptor
    generation = insert_generation!(command)

    binding =
      %AssetTargetBinding{
        workspace_id: command.workspace_context.workspace_id,
        target_id: command.target_id,
        active_generation_id: nil,
        desired_manifest_id: command.manifest_version_id,
        desired_descriptor_hash: descriptor.descriptor_hash,
        compatibility_status: "uninitialized",
        reason_code: "no_active_generation",
        compatibility_diff: %{},
        active_physical_fingerprint: nil,
        version: 1,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()

    writable_result(generation, binding)
  end

  defp resolve_binding!(binding, command, descriptor) do
    cond do
      binding.compatibility_status not in @writable_statuses ->
        Repo.rollback(
          Error.new(:conflict, "target compatibility blocks ordinary materialization",
            details: %{
              target_id: binding.target_id,
              compatibility_status: binding.compatibility_status,
              reason_code: binding.reason_code
            }
          )
        )

      binding.desired_descriptor_hash != descriptor.descriptor_hash or
          binding.desired_manifest_id != command.manifest_version_id ->
        Repo.rollback(Error.new(:conflict, "target binding is stale for the selected manifest"))

      is_binary(binding.active_generation_id) ->
        generation =
          lock_generation!(binding.workspace_id, binding.target_id, binding.active_generation_id)

        if generation.status == "active" do
          writable_result(generation, binding)
        else
          Repo.rollback(
            Error.new(:conflict, "active target binding references a non-active generation")
          )
        end

      true ->
        generation =
          existing_building_generation(
            binding.workspace_id,
            binding.target_id,
            descriptor.descriptor_hash
          ) || insert_generation!(command)

        if successful_generation_materialization?(generation) do
          Repo.rollback(
            Error.new(:conflict, "initial target generation reconciliation is required",
              details: %{
                reason_code: "initial_target_generation_reconciliation_required",
                target_id: binding.target_id,
                target_generation_id: generation.target_generation_id
              }
            )
          )
        else
          writable_result(generation, binding)
        end
    end
  end

  defp successful_generation_materialization?(generation) do
    from(materialization in Materialization,
      where:
        materialization.workspace_id == ^generation.workspace_id and
          materialization.target_kind == "asset" and
          materialization.target_id == ^generation.target_id and
          materialization.target_generation_id == ^generation.target_generation_id,
      select: true,
      limit: 1
    )
    |> Repo.exists?()
  end

  defp insert_generation!(command) do
    descriptor = command.descriptor

    %AssetTargetGeneration{
      workspace_id: command.workspace_context.workspace_id,
      target_id: command.target_id,
      target_generation_id: Ecto.UUID.generate(),
      creating_manifest_id: command.manifest_version_id,
      creation_command_id: command.command_id,
      creating_descriptor_hash: descriptor.descriptor_hash,
      active_descriptor_hash: nil,
      logical_relation: descriptor.relation,
      physical_relation: descriptor.relation,
      physical_schema_fingerprint: nil,
      data_plane_marker: nil,
      activation_token: nil,
      status: "building",
      creating_rebuild_operation_id: nil,
      version: 1,
      created_at: command.occurred_at,
      updated_at: command.occurred_at
    }
    |> Repo.insert!()
  end

  defp existing_building_generation(workspace_id, target_id, descriptor_hash) do
    from(generation in AssetTargetGeneration,
      where:
        generation.workspace_id == ^workspace_id and generation.target_id == ^target_id and
          generation.creating_descriptor_hash == ^descriptor_hash and
          generation.status == "building" and
          is_nil(generation.creating_rebuild_operation_id),
      order_by: [asc: generation.created_at],
      limit: 1,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp lock_target_identity!(workspace_id, target_id) do
    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1, 0))",
      ["favn:target-generation:" <> workspace_id <> ":" <> target_id]
    )

    :ok
  end

  defp lock_binding(workspace_id, target_id) do
    from(binding in AssetTargetBinding,
      where: binding.workspace_id == ^workspace_id and binding.target_id == ^target_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp lock_generation!(workspace_id, target_id, generation_id) do
    from(generation in AssetTargetGeneration,
      where:
        generation.workspace_id == ^workspace_id and generation.target_id == ^target_id and
          generation.target_generation_id == ^generation_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
    |> case do
      nil -> Repo.rollback(Error.new(:not_found, "target generation not found"))
      generation -> generation
    end
  end

  defp writable_result(generation, binding) do
    %WritableTargetGeneration{
      generation: generation_result(generation),
      binding: binding_result({binding, generation})
    }
  end

  defp initial_reconciliation_result(generation, binding, materialization_id) do
    %InitialTargetGenerationReconciliation{
      generation: generation_result(generation),
      binding: binding_result({binding, generation}),
      materialization_id: materialization_id
    }
  end

  defp generation_result(generation) do
    {:ok, result} =
      TargetGeneration.new(%{
        workspace_id: generation.workspace_id,
        target_id: generation.target_id,
        target_generation_id: generation.target_generation_id,
        creating_manifest_id: generation.creating_manifest_id,
        creating_descriptor_hash: generation.creating_descriptor_hash,
        active_descriptor_hash: generation.active_descriptor_hash,
        logical_relation: canonical_json_map(generation.logical_relation),
        physical_relation: canonical_json_map(generation.physical_relation),
        physical_schema_fingerprint: generation.physical_schema_fingerprint,
        data_plane_marker: canonical_json_map(generation.data_plane_marker),
        status: String.to_existing_atom(generation.status),
        rebuild_operation_id: generation.creating_rebuild_operation_id,
        version: generation.version,
        created_at: generation.created_at,
        activated_at: generation.activated_at,
        retired_at: generation.retired_at,
        updated_at: generation.updated_at
      })

    result
  end

  defp binding_result({binding, generation}) do
    active_generation =
      if generation && generation.target_generation_id == binding.active_generation_id,
        do: generation

    %TargetBinding{
      workspace_id: binding.workspace_id,
      target_id: binding.target_id,
      active_generation_id: binding.active_generation_id,
      active_manifest_id: active_generation && active_generation.creating_manifest_id,
      active_descriptor_hash: active_generation && active_generation.active_descriptor_hash,
      desired_manifest_id: binding.desired_manifest_id,
      desired_descriptor_hash: binding.desired_descriptor_hash,
      compatibility_status: String.to_existing_atom(binding.compatibility_status),
      reason_code: binding.reason_code,
      compatibility_diff: binding.compatibility_diff,
      active_physical_fingerprint: binding.active_physical_fingerprint,
      version: binding.version,
      updated_at: binding.updated_at
    }
  end

  defp validate_ensure(command) do
    with true <- WorkspaceContext.valid?(command.workspace_context),
         true <- valid_id?(command.command_id),
         true <- valid_id?(command.target_id),
         true <- valid_id?(command.manifest_version_id),
         {:ok, descriptor} <- TargetDescriptor.from_value(command.descriptor),
         true <- descriptor.target_id == command.target_id,
         true <- match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      _invalid -> {:error, Error.new(:invalid, "invalid writable target generation command")}
    end
  end

  defp validate_reconcile_initial(command) do
    if WorkspaceContext.valid?(command.workspace_context) and valid_id?(command.command_id) and
         valid_id?(command.target_id) and valid_id?(command.manifest_version_id) and
         valid_uuid?(command.target_generation_id) and valid_id?(command.materialization_id) and
         canonical_hash?(command.physical_schema_fingerprint) and
         valid_data_plane_marker?(
           command.data_plane_marker,
           command.target_id,
           command.target_generation_id
         ) and
         match?(%DateTime{}, command.occurred_at) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid initial target generation reconciliation command")}
    end
  end

  defp validate_get(context, target_ids) do
    if WorkspaceContext.valid?(context) and is_list(target_ids) and target_ids != [] and
         length(target_ids) <= @max_batch and length(target_ids) == length(Enum.uniq(target_ids)) and
         Enum.all?(target_ids, &valid_id?/1) do
      :ok
    else
      {:error, Error.new(:invalid, "invalid target binding query")}
    end
  end

  defp transaction(function) do
    case Repo.transaction(function) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  end

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255

  defp valid_uuid?(value), do: match?({:ok, _uuid}, Ecto.UUID.cast(value))

  defp canonical_hash?(value),
    do: is_binary(value) and byte_size(value) == 64 and value =~ ~r/\A[0-9a-f]{64}\z/

  defp bounded_json?(value, maximum) do
    value
    |> Jason.encode_to_iodata!()
    |> IO.iodata_length()
    |> Kernel.<=(maximum)
  rescue
    _error -> false
  end

  defp valid_data_plane_marker?(nil, _target_id, _target_generation_id), do: true

  defp valid_data_plane_marker?(marker, target_id, target_generation_id) do
    is_map(marker) and bounded_json?(marker, 65_536) and
      GenerationDataPlaneMarker.validate(marker, target_id, target_generation_id) == :ok
  end

  defp canonical_json_map(nil), do: nil

  defp canonical_json_map(value) do
    Map.new(value, fn {key, child} -> {to_string(key), canonical_json_value(child)} end)
  end

  defp canonical_json_value(%{} = value), do: canonical_json_map(value)

  defp canonical_json_value(value) when is_list(value),
    do: Enum.map(value, &canonical_json_value/1)

  defp canonical_json_value(value), do: value
end

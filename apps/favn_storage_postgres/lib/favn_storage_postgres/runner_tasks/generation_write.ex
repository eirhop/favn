defmodule FavnStoragePostgres.RunnerTasks.GenerationWrite do
  @moduledoc false
  import Ecto.Query
  alias Favn.Contracts.{GenerationCommit, GenerationMarker, GenerationPrecondition, RunnerWork}
  alias Favn.Contracts.RunnerTask.PersistenceData
  alias Favn.GenerationDataPlaneMarker
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.{CanonicalJSON, Repo}
  alias FavnStoragePostgres.Schemas.{AssetTargetBinding, AssetTargetGeneration}

  @limit 8_192

  # The caller holds target-operation authority. Metadata writers lock the binding
  # before its generation; no task or owner locks are acquired from this helper.
  def pin(task, %RunnerWork{target_operation: :normal_materialization} = work, now) do
    with {binding, generation} <- locked_state(task, work),
         true <- valid_state?(binding, generation, work) and eligible?(binding, work),
         {:ok, expected} <- precondition(binding, generation, work, now),
         :ok <- GenerationPrecondition.validate_work(expected, work),
         {:ok, encoded} <- PersistenceData.encode(expected, @limit) do
      {:ok, encoded}
    else
      _ -> {:error, :generation_no_longer_writable}
    end
  end

  def pin(_task, _work, _now), do: {:ok, nil}

  defp precondition(
         %{active_generation_id: nil},
         %{
           status: "building",
           creating_rebuild_operation_id: nil,
           data_plane_marker: nil,
           physical_schema_fingerprint: nil,
           creating_descriptor_hash: hash
         },
         %{target_descriptor_hash: hash} = work,
         now
       ) do
    {:ok,
     %GenerationPrecondition{
       mode: :initial,
       marker: %GenerationMarker{
         target_id: work.logical_target_id,
         active_relation: work.write_relation,
         active_generation_id: work.target_generation_id,
         activation_operation_id: "initial:" <> work.target_generation_id,
         activation_token: "initial:" <> work.target_generation_id,
         activated_at: now
       }
     }}
  end

  defp precondition(
         %{active_generation_id: id},
         %{status: "active"} = generation,
         %{target_generation_id: id} = work,
         _now
       ) do
    with {:ok, marker} <- marker(generation.data_plane_marker, work) do
      {:ok,
       %GenerationPrecondition{
         mode: :existing,
         marker: marker,
         physical_fingerprint: generation.physical_schema_fingerprint
       }}
    end
  end

  defp precondition(_, _, _, _), do: {:error, :generation_no_longer_writable}

  def decode(nil, _work), do: {:ok, nil}

  def decode(encoded, %RunnerWork{write_relation: %{connection: connection}} = work) do
    with {:ok, expected} <- PersistenceData.decode(encoded, @limit, nil, [connection]),
         :ok <- GenerationPrecondition.validate_work(expected, work),
         do: {:ok, expected}
  end

  def decode(_, _), do: {:error, :invalid_generation_precondition}

  def start!(task, %RunnerWork{target_operation: :normal_materialization} = work) do
    {binding, generation} = locked_state!(task, work)
    require!(eligible?(binding, work))
    expected = decode!(task.generation_precondition, work)
    require!(matches_state?(expected, binding, generation))
  end

  def start!(_task, _work), do: :ok

  def validate_result!(task, %RunnerWork{target_operation: :normal_materialization} = work, %{
        status: :ok,
        asset_results: [asset]
      }) do
    expected = decode!(task.generation_precondition, work)
    require!(match?(%Favn.Contracts.RunnerAssetEvidence{kind: :sql}, asset.evidence))

    require!(
      asset.evidence.write_outcome == :written or
        (expected.mode == :existing and asset.evidence.write_outcome == :no_op)
    )

    receipt = asset.evidence.generation_commit
    require!(GenerationCommit.validate(receipt, expected) == :ok)
    :ok
  end

  def validate_result!(_task, _work, _result), do: :ok

  def complete!(
        task,
        %RunnerWork{target_operation: :normal_materialization} = work,
        %{status: :ok, asset_results: [asset]},
        now
      ) do
    expected = decode!(task.generation_precondition, work)
    receipt = asset.evidence.generation_commit
    {binding, generation} = locked_state!(task, work)
    require!(matches_state?(expected, binding, generation))

    if expected.mode == :initial do
      generation
      |> Ecto.Changeset.change(%{
        active_descriptor_hash: work.target_descriptor_hash,
        physical_schema_fingerprint: receipt.physical_fingerprint,
        data_plane_marker: canonical(receipt.marker),
        activation_token: receipt.marker.activation_token,
        status: "active",
        version: generation.version + 1,
        activated_at: now,
        updated_at: now
      })
      |> Repo.update!()

      ready? =
        binding.desired_manifest_id == work.manifest_version_id and
          binding.desired_descriptor_hash == work.target_descriptor_hash and
          binding.compatibility_status == "uninitialized"

      binding
      |> Ecto.Changeset.change(%{
        active_generation_id: work.target_generation_id,
        active_physical_fingerprint: receipt.physical_fingerprint,
        compatibility_status: if(ready?, do: "ready", else: "operator_decision"),
        reason_code:
          if(ready?,
            do: "initial_materialization_committed",
            else: "deployment_changed_during_write"
          ),
        compatibility_diff: %{},
        version: binding.version + 1,
        updated_at: now
      })
      |> Repo.update!()
    end

    :ok
  end

  def complete!(_task, _work, _result, _now), do: :ok

  defp decode!(encoded, work) do
    case decode(encoded, work) do
      {:ok, %GenerationPrecondition{} = expected} -> expected
      _ -> reject!()
    end
  end

  defp locked_state!(task, work) do
    {binding, generation} = locked_state(task, work)
    require!(valid_state?(binding, generation, work))
    {binding, generation}
  end

  defp locked_state(task, work) do
    binding =
      Repo.one(
        from(b in AssetTargetBinding,
          where: b.workspace_id == ^task.workspace_id and b.target_id == ^work.logical_target_id,
          lock: "FOR UPDATE"
        )
      )

    generation =
      Repo.one(
        from(g in AssetTargetGeneration,
          where:
            g.workspace_id == ^task.workspace_id and g.target_id == ^work.logical_target_id and
              g.target_generation_id == ^work.target_generation_id,
          lock: "FOR UPDATE"
        )
      )

    {binding, generation}
  end

  defp valid_state?(nil, _, _), do: false
  defp valid_state?(_, nil, _), do: false

  defp valid_state?(_, generation, work),
    do: canonical(generation.physical_relation) == canonical(work.write_relation)

  defp eligible?(binding, work),
    do:
      binding.compatibility_status in ~w(ready uninitialized rebuild_available) and
        binding.desired_manifest_id == work.manifest_version_id and
        binding.desired_descriptor_hash == work.target_descriptor_hash

  defp matches_state?(%{mode: :initial, marker: marker}, binding, generation),
    do:
      is_nil(binding.active_generation_id) and generation.status == "building" and
        generation.target_generation_id == marker.active_generation_id and
        is_nil(generation.creating_rebuild_operation_id) and is_nil(generation.data_plane_marker) and
        is_nil(generation.physical_schema_fingerprint)

  defp matches_state?(%{mode: :existing} = expected, binding, generation),
    do:
      binding.active_generation_id == generation.target_generation_id and
        generation.status == "active" and
        canonical(expected.marker) == generation.data_plane_marker and
        expected.physical_fingerprint == generation.physical_schema_fingerprint

  defp marker(data, work) do
    with :ok <-
           GenerationDataPlaneMarker.validate(
             data,
             work.logical_target_id,
             work.target_generation_id
           ),
         data <- canonical(data),
         true <- data["active_relation"] == canonical(work.write_relation),
         {:ok, timestamp, 0} <- DateTime.from_iso8601(data["activated_at"]) do
      {:ok,
       %GenerationMarker{
         target_id: work.logical_target_id,
         active_relation: work.write_relation,
         active_generation_id: work.target_generation_id,
         activation_operation_id: data["activation_operation_id"],
         activation_token: data["activation_token"],
         activated_at: timestamp
       }}
    else
      _ -> {:error, :invalid_generation_marker}
    end
  end

  defp canonical(value) do
    {:ok, json} = CanonicalJSON.encode(value)
    Jason.decode!(json)
  end

  defp require!(true), do: :ok
  defp require!(_), do: reject!()

  defp reject!,
    do:
      Repo.rollback(
        Error.new(:fenced, "assignment generation evidence does not match target authority")
      )
end

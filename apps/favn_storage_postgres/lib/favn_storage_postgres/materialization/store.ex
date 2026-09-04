defmodule FavnStoragePostgres.Materialization.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.MaterializationStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Commands.RenewMaterializationClaim
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetMaterializations
  alias FavnOrchestrator.Persistence.Queries.GetRebuildMaterialization
  alias FavnOrchestrator.Persistence.Results.Materialization, as: MaterializationResult
  alias FavnOrchestrator.Persistence.Results.MaterializationClaim, as: ClaimResult
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.WriteOwnership
  alias FavnStoragePostgres.Schemas.AssetEvidenceBinding
  alias FavnStoragePostgres.Schemas.Materialization
  alias FavnStoragePostgres.Schemas.MaterializationClaim
  alias FavnStoragePostgres.Schemas.TargetOperationLock

  @max_batch 500

  @impl true
  def claim(%ClaimMaterialization{} = command) do
    with true <- command.purpose in [:materialization, :ownership_only],
         :ok <- validate_claim(command) do
      transaction(fn -> claim!(command) end)
    else
      false -> {:error, Error.new(:invalid, "invalid claim purpose")}
      {:error, _} = error -> error
    end
  end

  @impl true
  def renew(%RenewMaterializationClaim{} = command) do
    with :ok <- validate_renew(command) do
      transaction(fn -> renew!(command) end)
    end
  end

  @impl true
  def finish(%FinishMaterialization{} = command) do
    with :ok <- validate_finish(command) do
      transaction(fn -> finish!(command) end)
    end
  end

  @impl true
  def get_many(%GetMaterializations{} = query) do
    with :ok <- validate_get_many(query) do
      workspace_id = query.workspace_context.workspace_id

      claims =
        from(claim in MaterializationClaim,
          where: claim.workspace_id == ^workspace_id and claim.claim_key in ^query.claim_keys
        )
        |> Repo.all()
        |> Map.new(&{&1.claim_key, &1})

      materializations =
        from(materialization in Materialization,
          where:
            materialization.workspace_id == ^workspace_id and
              materialization.claim_key in ^query.claim_keys
        )
        |> Repo.all()
        |> Map.new(&{&1.claim_key, &1})

      {:ok,
       Enum.map(query.claim_keys, fn claim_key ->
         lookup_decision(
           claim_key,
           Map.get(claims, claim_key),
           Map.get(materializations, claim_key)
         )
       end)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_rebuild(%GetRebuildMaterialization{} = query) do
    with :ok <- validate_get_rebuild(query) do
      workspace_id = query.workspace_context.workspace_id

      claim =
        from(claim in MaterializationClaim,
          where:
            claim.workspace_id == ^workspace_id and
              claim.operation_id == ^query.operation_id and
              claim.target_id == ^query.target_id and claim.run_id == ^query.run_id and
              claim.target_generation_id == ^query.target_generation_id and
              claim.partition_key == ^query.partition_key,
          limit: 1
        )
        |> Repo.one()

      materialization =
        if claim do
          from(materialization in Materialization,
            where:
              materialization.workspace_id == ^workspace_id and
                materialization.claim_key == ^claim.claim_key,
            limit: 1
          )
          |> Repo.one()
        end

      {:ok, lookup_decision(query.run_id, claim, materialization)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp claim!(command) do
    workspace_id = command.workspace_context.workspace_id
    lock_target_operation_identity!(workspace_id, command.target_id)
    lock_claim_identity!(workspace_id, command.claim_key)

    case lock_materialization(workspace_id, command.claim_key) do
      %Materialization{} = materialization ->
        materialized_decision(materialization)

      nil ->
        request_hash = claim_request_hash!(command)
        claim = lock_claim(workspace_id, command.claim_key)

        if claim && exact_live_claim_replay?(claim, command, request_hash) do
          claimed_decision(claim)
        else
          ensure_target_operation_admission!(command)
          ensure_evidence_binding!(command)

          case claim do
            nil -> insert_claim!(command, request_hash)
            claim -> resolve_existing_claim!(claim, command, request_hash)
          end
        end
    end
  end

  defp lock_target_operation_identity!(workspace_id, target_id) do
    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1, 0))",
      ["favn:target-operation:" <> workspace_id <> ":" <> target_id]
    )

    :ok
  end

  defp ensure_target_operation_admission!(command) do
    WriteOwnership.guard_target!(command.workspace_context.workspace_id, command.target_id)

    operation_lock =
      from(lock in TargetOperationLock,
        where:
          lock.workspace_id == ^command.workspace_context.workspace_id and
            lock.target_id == ^command.target_id and
            lock.lease_expires_at > fragment("clock_timestamp()"),
        lock: "FOR SHARE"
      )
      |> Repo.one()

    competing_claim = competing_target_claim(command)

    if (is_nil(operation_lock) or operation_lock.operation_id == command.operation_id) and
         is_nil(competing_claim) do
      :ok
    else
      operation_id =
        if operation_lock,
          do: operation_lock.operation_id,
          else: competing_claim.operation_id

      Repo.rollback(
        Error.new(:conflict, "target operation is in progress",
          details: %{
            reason_code: "target_operation_in_progress",
            target_id: command.target_id,
            operation_id: operation_id
          }
        )
      )
    end
  end

  defp competing_target_claim(command) do
    base =
      from(claim in MaterializationClaim,
        where:
          claim.workspace_id == ^command.workspace_context.workspace_id and
            claim.target_id == ^command.target_id and claim.status == "claimed" and
            claim.expires_at > fragment("clock_timestamp()"),
        limit: 1,
        lock: "FOR SHARE"
      )

    query =
      if is_nil(command.operation_id) do
        from(claim in base, where: not is_nil(claim.operation_id))
      else
        from(claim in base,
          where: is_nil(claim.operation_id) or claim.operation_id != ^command.operation_id
        )
      end

    Repo.one(query)
  end

  defp ensure_evidence_binding!(%{target_generation_id: generation_id})
       when is_binary(generation_id),
       do: :ok

  defp ensure_evidence_binding!(%{target_kind: :pipeline}), do: :ok

  defp ensure_evidence_binding!(command) do
    binding =
      from(binding in AssetEvidenceBinding,
        where:
          binding.workspace_id == ^command.workspace_context.workspace_id and
            binding.target_id == ^command.target_id,
        lock: "FOR KEY SHARE"
      )
      |> Repo.one()

    if match?(%AssetEvidenceBinding{}, binding) and
         binding.evidence_generation_id == command.evidence_generation_id do
      :ok
    else
      Repo.rollback(
        Error.new(:conflict, "asset evidence binding does not match the materialization claim",
          details: %{
            reason_code: "evidence_binding_mismatch",
            target_id: command.target_id
          }
        )
      )
    end
  end

  defp lock_claim_identity!(workspace_id, claim_key) do
    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1, 0))",
      ["favn:materialization:" <> workspace_id <> ":" <> claim_key]
    )

    :ok
  end

  defp insert_claim!(command, request_hash) do
    claim =
      %MaterializationClaim{
        workspace_id: command.workspace_context.workspace_id,
        claim_key: command.claim_key,
        deployment_id: command.deployment_id,
        target_kind: Atom.to_string(command.target_kind),
        target_id: command.target_id,
        target_generation_id: command.target_generation_id,
        evidence_generation_id: command.evidence_generation_id,
        partition_key: command.partition_key,
        run_id: command.run_id,
        operation_id: command.operation_id,
        claim_command_id: command.command_id,
        claim_request_hash: request_hash,
        owner_id: command.owner_id,
        fencing_token: 1,
        status: "claimed",
        purpose: Atom.to_string(command.purpose),
        expires_at: database_deadline!(command.lease_duration_ms),
        version: 1,
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }
      |> Repo.insert!()

    claimed_decision(claim)
  end

  defp resolve_existing_claim!(claim, command, request_hash) do
    cond do
      exact_live_claim_replay?(claim, command, request_hash) ->
        claimed_decision(claim)

      WriteOwnership.unresolved?(claim) or
          (claim.status == "claimed" and future?(claim.expires_at)) ->
        %MaterializationDecision{
          claim_key: claim.claim_key,
          status: :competing,
          claim: claim_result(claim)
        }

      same_logical_identity?(claim, command) ->
        reclaimed =
          claim
          |> Ecto.Changeset.change(%{
            effect_state: "not_started",
            effect_task_id: nil,
            effect_assignment_generation: nil,
            effect_started_at: nil,
            effect_resolution: nil,
            claim_command_id: command.command_id,
            claim_request_hash: request_hash,
            owner_id: command.owner_id,
            run_id: command.run_id,
            operation_id: command.operation_id,
            target_generation_id: command.target_generation_id,
            evidence_generation_id: command.evidence_generation_id,
            fencing_token: claim.fencing_token + 1,
            last_renewal_id: nil,
            last_finish_command_id: nil,
            finish_hash: nil,
            status: "claimed",
            expires_at: database_deadline!(command.lease_duration_ms),
            completed_at: nil,
            result: nil,
            error: nil,
            version: claim.version + 1,
            updated_at: command.occurred_at
          })
          |> Repo.update!()

        claimed_decision(reclaimed)

      true ->
        Repo.rollback(
          Error.new(:conflict, "materialization claim identity has different content")
        )
    end
  end

  defp renew!(command) do
    workspace_id = command.workspace_context.workspace_id
    claim = lock_claim!(workspace_id, command.claim_key)

    cond do
      claim.last_renewal_id == command.renewal_id and matching_owner?(claim, command) and
        claim.status == "claimed" and future?(claim.expires_at) ->
        claim_result(claim)

      not matching_owner?(claim, command) or claim.status != "claimed" or
          not future?(claim.expires_at) ->
        Repo.rollback(Error.new(:fenced, "materialization claim cannot be renewed"))

      true ->
        %{rows: [row]} =
          SQL.query!(
            Repo,
            """
            UPDATE favn_control.materialization_claims
            SET last_renewal_id = $5,
                expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
                updated_at = clock_timestamp()
            WHERE workspace_id = $1 AND claim_key = $2 AND owner_id = $3
              AND fencing_token = $4 AND status = 'claimed'
              AND expires_at > clock_timestamp()
            RETURNING workspace_id, claim_key, deployment_id, target_kind, target_id,
                      target_generation_id, evidence_generation_id, partition_key, run_id,
                      owner_id, fencing_token, status, expires_at,
                      completed_at, result, error, version, purpose
            """,
            [
              workspace_id,
              command.claim_key,
              command.owner_id,
              command.fencing_token,
              command.renewal_id,
              command.lease_duration_ms
            ]
          )

        claim_result(row)
    end
  end

  defp purpose("ownership_only"), do: :ownership_only
  defp purpose("materialization"), do: :materialization

  defp finish!(%{status: :released} = command) do
    workspace = command.workspace_context.workspace_id

    original =
      Repo.get_by!(MaterializationClaim, workspace_id: workspace, claim_key: command.claim_key)

    WriteOwnership.lock_target!(workspace, original.target_id)
    claim = lock_claim!(workspace, command.claim_key)
    hash = finish_hash!(command)

    cond do
      claim.last_finish_command_id == command.command_id and claim.finish_hash == hash ->
        lookup_decision(command.claim_key, claim, nil)

      claim.purpose == "ownership_only" and claim.status == "claimed" and
        matching_owner?(claim, command) and claim.version == command.expected_version and
        claim.effect_state == "not_started" and is_nil(claim.effect_task_id) ->
        released =
          claim
          |> Ecto.Changeset.change(
            status: "released",
            completed_at: command.occurred_at,
            updated_at: command.occurred_at,
            version: claim.version + 1,
            last_finish_command_id: command.command_id,
            finish_hash: hash
          )
          |> Repo.update!()

        lookup_decision(command.claim_key, released, nil)

      true ->
        Repo.rollback(Error.new(:fenced, "only an exact unbound ownership claim can be released"))
    end
  end

  defp finish!(command) do
    workspace_id = command.workspace_context.workspace_id

    original =
      Repo.get_by!(MaterializationClaim, workspace_id: workspace_id, claim_key: command.claim_key)

    WriteOwnership.lock_target!(workspace_id, original.target_id)
    claim = lock_claim!(workspace_id, command.claim_key)

    if WriteOwnership.unresolved?(claim),
      do:
        Repo.rollback(
          Error.new(
            :conflict,
            "materialization write outcome remains unresolved",
            details: %{reason_code: "target_write_outcome_unknown"}
          )
        )

    if claim.purpose != "materialization",
      do:
        Repo.rollback(Error.new(:conflict, "ownership-only claims settle with their runner task"))

    finish_hash = finish_hash!(command)

    cond do
      claim.last_finish_command_id == command.command_id and claim.finish_hash == finish_hash ->
        lookup_decision(
          command.claim_key,
          claim,
          lock_materialization(workspace_id, command.claim_key)
        )

      claim.last_finish_command_id == command.command_id ->
        Repo.rollback(
          Error.new(:conflict, "materialization finish command has different content")
        )

      not matching_owner?(claim, command) or claim.status != "claimed" or
          not (future?(claim.expires_at) or
                   WriteOwnership.committed_settlement?(claim, command.status)) ->
        Repo.rollback(Error.new(:fenced, "materialization claim fencing token is stale"))

      claim.version != command.expected_version ->
        Repo.rollback(Error.new(:conflict, "materialization claim version changed"))

      true ->
        commit_finish!(claim, command, finish_hash)
    end
  end

  defp commit_finish!(claim, %{status: :succeeded} = command, finish_hash) do
    payload_hash = hash!(command.payload)

    outbox =
      OutboxWriter.insert!(%{
        workspace_id: claim.workspace_id,
        command_id: command.command_id,
        event_kind: "materialization.succeeded",
        aggregate_kind: "materialization",
        aggregate_id: command.materialization_id,
        aggregate_version: claim.version + 1,
        occurred_at: command.occurred_at,
        payload: %{
          "materialization_id" => command.materialization_id,
          "claim_key" => claim.claim_key,
          "run_id" => claim.run_id,
          "target_id" => claim.target_id,
          "target_generation_id" => claim.target_generation_id,
          "evidence_generation_id" => claim.evidence_generation_id
        }
      })

    materialization =
      %Materialization{
        workspace_id: claim.workspace_id,
        materialization_id: command.materialization_id,
        claim_key: claim.claim_key,
        deployment_id: claim.deployment_id,
        target_kind: claim.target_kind,
        target_id: claim.target_id,
        target_generation_id: claim.target_generation_id,
        evidence_generation_id: claim.evidence_generation_id,
        partition_key: claim.partition_key,
        run_id: claim.run_id,
        payload: command.payload,
        payload_hash: payload_hash,
        outbox_event_id: outbox.outbox_event_id,
        inserted_at: command.occurred_at
      }
      |> Repo.insert!()

    claim
    |> Ecto.Changeset.change(%{
      last_finish_command_id: command.command_id,
      finish_hash: finish_hash,
      status: "succeeded",
      completed_at: command.occurred_at,
      result: command.payload,
      error: nil,
      version: claim.version + 1,
      updated_at: command.occurred_at
    })
    |> Repo.update!()

    materialized_decision(materialization)
  end

  defp commit_finish!(claim, %{status: :failed} = command, finish_hash) do
    updated =
      claim
      |> Ecto.Changeset.change(%{
        last_finish_command_id: command.command_id,
        finish_hash: finish_hash,
        status: "failed",
        completed_at: command.occurred_at,
        result: nil,
        error: command.error,
        version: claim.version + 1,
        updated_at: command.occurred_at
      })
      |> Repo.update!()

    OutboxWriter.insert!(%{
      workspace_id: claim.workspace_id,
      command_id: command.command_id,
      event_kind: "materialization.failed",
      aggregate_kind: "materialization_claim",
      aggregate_id: claim.claim_key,
      aggregate_version: updated.version,
      occurred_at: command.occurred_at,
      payload: %{
        "claim_key" => claim.claim_key,
        "run_id" => claim.run_id,
        "target_id" => claim.target_id,
        "status" => "failed"
      }
    })

    %MaterializationDecision{
      claim_key: claim.claim_key,
      status: :failed,
      claim: claim_result(updated)
    }
  end

  defp lock_claim(workspace_id, claim_key) do
    from(claim in MaterializationClaim,
      where: claim.workspace_id == ^workspace_id and claim.claim_key == ^claim_key,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp lock_claim!(workspace_id, claim_key) do
    case lock_claim(workspace_id, claim_key) do
      nil -> Repo.rollback(Error.new(:not_found, "materialization claim not found"))
      claim -> claim
    end
  end

  defp lock_materialization(workspace_id, claim_key) do
    from(materialization in Materialization,
      where:
        materialization.workspace_id == ^workspace_id and
          materialization.claim_key == ^claim_key,
      lock: "FOR KEY SHARE"
    )
    |> Repo.one()
  end

  defp lookup_decision(_claim_key, _claim, %Materialization{} = materialization),
    do: materialized_decision(materialization)

  defp lookup_decision(claim_key, nil, nil),
    do: %MaterializationDecision{claim_key: claim_key, status: :missing}

  defp lookup_decision(_claim_key, %MaterializationClaim{status: "released"} = claim, nil),
    do: %MaterializationDecision{
      claim_key: claim.claim_key,
      status: :released,
      claim: claim_result(claim)
    }

  defp lookup_decision(_claim_key, %MaterializationClaim{status: "failed"} = claim, nil),
    do: %MaterializationDecision{
      claim_key: claim.claim_key,
      status: :failed,
      claim: claim_result(claim)
    }

  defp lookup_decision(_claim_key, %MaterializationClaim{status: "claimed"} = claim, nil) do
    if future?(claim.expires_at) do
      claimed_decision(claim)
    else
      %MaterializationDecision{
        claim_key: claim.claim_key,
        status: :expired,
        claim: claim |> claim_result() |> Map.put(:status, :expired)
      }
    end
  end

  defp lookup_decision(_claim_key, %MaterializationClaim{} = claim, nil),
    do: claimed_decision(claim)

  defp claimed_decision(claim) do
    %MaterializationDecision{
      claim_key: claim.claim_key,
      status: :claimed,
      claim: claim_result(claim)
    }
  end

  defp materialized_decision(materialization) do
    %MaterializationDecision{
      claim_key: materialization.claim_key,
      status: :materialized,
      materialization: materialization_result(materialization)
    }
  end

  defp claim_result(%MaterializationClaim{} = claim) do
    %ClaimResult{
      workspace_id: claim.workspace_id,
      claim_key: claim.claim_key,
      purpose: purpose(claim.purpose),
      deployment_id: claim.deployment_id,
      target_kind: String.to_existing_atom(claim.target_kind),
      target_id: claim.target_id,
      target_generation_id: claim.target_generation_id,
      evidence_generation_id: claim.evidence_generation_id,
      partition_key: claim.partition_key,
      run_id: claim.run_id,
      owner_id: claim.owner_id,
      fencing_token: claim.fencing_token,
      status: String.to_existing_atom(claim.status),
      expires_at: claim.expires_at,
      completed_at: claim.completed_at,
      result: claim.result,
      error: claim.error,
      version: claim.version
    }
  end

  defp claim_result([
         workspace_id,
         claim_key,
         deployment_id,
         target_kind,
         target_id,
         target_generation_id,
         evidence_generation_id,
         partition_key,
         run_id,
         owner_id,
         fencing_token,
         status,
         expires_at,
         completed_at,
         result,
         error,
         version,
         claim_purpose
       ]) do
    %ClaimResult{
      workspace_id: workspace_id,
      claim_key: claim_key,
      purpose: purpose(claim_purpose),
      deployment_id: deployment_id,
      target_kind: String.to_existing_atom(target_kind),
      target_id: target_id,
      target_generation_id: target_generation_id,
      evidence_generation_id: evidence_generation_id,
      partition_key: partition_key,
      run_id: run_id,
      owner_id: owner_id,
      fencing_token: fencing_token,
      status: String.to_existing_atom(status),
      expires_at: expires_at,
      completed_at: completed_at,
      result: result,
      error: error,
      version: version
    }
  end

  defp materialization_result(materialization) do
    %MaterializationResult{
      workspace_id: materialization.workspace_id,
      materialization_id: materialization.materialization_id,
      claim_key: materialization.claim_key,
      deployment_id: materialization.deployment_id,
      target_kind: String.to_existing_atom(materialization.target_kind),
      target_id: materialization.target_id,
      target_generation_id: materialization.target_generation_id,
      evidence_generation_id: materialization.evidence_generation_id,
      partition_key: materialization.partition_key,
      run_id: materialization.run_id,
      payload: materialization.payload,
      inserted_at: materialization.inserted_at
    }
  end

  defp exact_live_claim_replay?(claim, command, request_hash) do
    claim.claim_command_id == command.command_id and
      claim.claim_request_hash == request_hash and same_logical_identity?(claim, command) and
      claim.owner_id == command.owner_id and claim.status == "claimed" and
      future?(claim.expires_at)
  end

  defp same_logical_identity?(claim, command) do
    claim.claim_key == command.claim_key and claim.deployment_id == command.deployment_id and
      claim.target_kind == Atom.to_string(command.target_kind) and
      claim.target_id == command.target_id and
      claim.target_generation_id == command.target_generation_id and
      claim.evidence_generation_id == command.evidence_generation_id and
      claim.partition_key == command.partition_key and
      claim.purpose == Atom.to_string(command.purpose)
  end

  defp matching_owner?(claim, command),
    do:
      claim.owner_id == command.owner_id and
        claim.fencing_token == command.fencing_token

  defp claim_request_hash!(command) do
    hash!(%{
      claim_key: command.claim_key,
      deployment_id: command.deployment_id,
      target_kind: command.target_kind,
      target_id: command.target_id,
      target_generation_id: command.target_generation_id,
      evidence_generation_id: command.evidence_generation_id,
      partition_key: command.partition_key,
      run_id: command.run_id,
      owner_id: command.owner_id,
      operation_id: command.operation_id,
      purpose: command.purpose
    })
  end

  defp finish_hash!(command) do
    hash!(%{
      claim_key: command.claim_key,
      status: command.status,
      materialization_id: command.materialization_id,
      payload: command.payload,
      error: command.error
    })
  end

  defp hash!(value) do
    {:ok, hash} = CanonicalJSON.hash(value)
    hash
  end

  defp database_deadline!(duration_ms) do
    %{rows: [[deadline]]} =
      SQL.query!(
        Repo,
        "SELECT clock_timestamp() + ($1 * interval '1 millisecond')",
        [duration_ms]
      )

    deadline
  end

  defp future?(timestamp) do
    %{rows: [[future?]]} =
      SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [timestamp])

    future?
  end

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_claim(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?(
           [
             command.command_id,
             command.claim_key,
             command.deployment_id,
             command.target_id,
             command.evidence_generation_id,
             command.partition_key,
             command.run_id,
             command.owner_id
           ],
           &valid_id?/1
         ) and command.target_kind in [:asset, :pipeline] and
         command.purpose in [:materialization, :ownership_only] and
         valid_generation_identity?(command.target_generation_id, command.evidence_generation_id) and
         valid_optional_id?(command.operation_id) and
         valid_duration?(command.lease_duration_ms) and match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_renew(command) do
    if workspace_context?(command.workspace_context) and
         Enum.all?([command.renewal_id, command.claim_key, command.owner_id], &valid_id?/1) and
         is_integer(command.fencing_token) and command.fencing_token > 0 and
         valid_duration?(command.lease_duration_ms),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_finish(command) do
    if valid_finish_command?(command) and valid_finish_payload?(command),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_finish_command?(command) do
    workspace_context?(command.workspace_context) and
      Enum.all?([command.command_id, command.claim_key, command.owner_id], &valid_id?/1) and
      is_integer(command.fencing_token) and command.fencing_token > 0 and
      is_integer(command.expected_version) and command.expected_version > 0 and
      command.status in [:succeeded, :failed, :released] and
      match?(%DateTime{}, command.occurred_at)
  end

  defp valid_finish_payload?(%{status: :succeeded} = command) do
    valid_id?(command.materialization_id) and is_map(command.payload) and
      Payload.validate(command.payload, 256 * 1_024) == :ok
  end

  defp valid_finish_payload?(%{status: :failed} = command) do
    is_map(command.error) and Payload.validate(command.error, 64 * 1_024) == :ok
  end

  defp valid_finish_payload?(%{
         status: :released,
         materialization_id: nil,
         payload: nil,
         error: nil
       }),
       do: true

  defp valid_finish_payload?(_command), do: false

  defp validate_get_many(query) do
    keys = query.claim_keys

    if workspace_context?(query.workspace_context) and is_list(keys) and keys != [] and
         length(keys) <= @max_batch and Enum.all?(keys, &valid_id?/1) and
         length(keys) == length(Enum.uniq(keys)),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_get_rebuild(query) do
    valid =
      workspace_context?(query.workspace_context) and valid_id?(query.operation_id) and
        valid_id?(query.target_id) and valid_id?(query.run_id) and
        valid_id?(query.target_generation_id) and valid_id?(query.partition_key)

    if valid, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_duration?(duration), do: is_integer(duration) and duration > 0
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
  defp valid_optional_id?(nil), do: true
  defp valid_optional_id?(value), do: valid_id?(value)

  defp valid_generation_identity?(nil, evidence_generation_id),
    do: valid_id?(evidence_generation_id)

  defp valid_generation_identity?(target_generation_id, evidence_generation_id) do
    target_generation_id == evidence_generation_id and
      match?({:ok, _uuid}, Ecto.UUID.cast(target_generation_id))
  end
end

defmodule FavnStoragePostgres.CancellationOwnership do
  @moduledoc false

  import Ecto.Query
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.RunSubmission.Intent
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunIdentity

  alias FavnStoragePostgres.Schemas.{
    Backfill,
    BackfillWindow,
    ResourceRecoveryCandidate,
    Run,
    RunSubmission
  }

  def owner!(workspace_id, run_id) do
    case member(workspace_id, run_id) do
      %{cancellation_owner_run_id: owner} when is_binary(owner) -> owner
      nil -> Repo.rollback(Error.new(:not_found, "cancellation authority not found"))
      _legacy -> invalid!()
    end
  end

  def new_owner!(workspace_id, run_id) do
    case member(workspace_id, run_id) do
      nil -> run_id
      %{cancellation_owner_run_id: owner} when is_binary(owner) -> owner
      _legacy -> invalid!()
    end
  end

  defp member(workspace, run_id) do
    Repo.one(
      from(s in RunSubmission,
        where: s.workspace_id == ^workspace and s.run_id == ^run_id,
        select: %{cancellation_owner_run_id: s.cancellation_owner_run_id}
      )
    ) ||
      Repo.one(
        from(r in Run,
          where: r.workspace_id == ^workspace and r.run_id == ^run_id,
          select: %{cancellation_owner_run_id: r.cancellation_owner_run_id}
        )
      )
  end

  def lock_new!(workspace_id, run_id) do
    owner = new_owner!(workspace_id, run_id)
    RunIdentity.lock!(workspace_id, owner)
    if owner != run_id, do: RunIdentity.lock!(workspace_id, run_id)
    owner
  end

  def guard_new!(workspace_id, run_id) do
    lock_new!(workspace_id, run_id)

    if Repo.exists?(
         from(s in RunSubmission, where: s.workspace_id == ^workspace_id and s.run_id == ^run_id)
       ) or
         Repo.exists?(
           from(r in Run, where: r.workspace_id == ^workspace_id and r.run_id == ^run_id)
         ) do
      guard!(workspace_id, run_id)
    end

    :ok
  end

  def lock!(workspace_id, run_id) do
    owner = owner!(workspace_id, run_id)
    RunIdentity.lock!(workspace_id, owner)
    if owner != run_id, do: RunIdentity.lock!(workspace_id, run_id)
    owner
  end

  def try_lock!(workspace_id, run_id) do
    owner = owner!(workspace_id, run_id)

    RunIdentity.try_lock!(workspace_id, owner) and
      (owner == run_id or RunIdentity.try_lock!(workspace_id, run_id))
  end

  def guard!(workspace_id, run_id) do
    lock!(workspace_id, run_id)
    if cancelled?(workspace_id, run_id), do: Repo.rollback(cancelled_error())
    :ok
  end

  def cancelled?(_workspace_id, nil), do: false

  def cancelled?(workspace_id, run_id) do
    owner = owner!(workspace_id, run_id)
    backfill = backfill!(workspace_id, owner)
    owner_run = cancellation_flags(Run, workspace_id, owner)
    leaf = if owner == run_id, do: owner_run, else: cancellation_flags(Run, workspace_id, run_id)
    owner_submission = cancellation_flags(RunSubmission, workspace_id, owner)

    submission =
      if owner == run_id,
        do: owner_submission,
        else: cancellation_flags(RunSubmission, workspace_id, run_id)

    if is_nil(backfill) and is_nil(owner_run) and is_nil(owner_submission), do: invalid!()

    not is_nil(backfill && backfill.cancellation_requested_at) or
      not is_nil(owner_run && owner_run.cancellation_status) or
      not is_nil(leaf && leaf.cancellation_requested_at) or
      not is_nil(submission && submission.cancellation_requested_at) or
      (is_nil(owner_run) and
         not is_nil(owner_submission && owner_submission.cancellation_requested_at))
  end

  defp cancellation_flags(schema, workspace, run_id) do
    fields =
      if schema == Run,
        do: [:cancellation_requested_at, :cancellation_status],
        else: [:cancellation_requested_at]

    Repo.one(
      from(r in schema,
        where: r.workspace_id == ^workspace and r.run_id == ^run_id,
        select: map(r, ^fields)
      )
    )
  end

  def backfill!(workspace_id, owner) do
    case Repo.all(
           from(b in Backfill,
             where: b.workspace_id == ^workspace_id and b.root_run_id == ^owner,
             limit: 2
           )
         ) do
      [] -> nil
      [backfill] -> backfill
      _ambiguous -> invalid!()
    end
  end

  def submission_owner!(workspace_id, run_id, source, intent) do
    case to_string(source) do
      "backfill" -> backfill_owner!(workspace_id, run_id, intent)
      "recovery" -> recovery_owner!(workspace_id, run_id, intent)
      _explicit_submission -> run_id
    end
  end

  defp backfill_owner!(workspace_id, run_id, intent) do
    with {:ok, {_operation, _selector, opts}} <- Intent.decode(intent),
         metadata <- Keyword.get(opts, :metadata, %{}),
         backfill_id when is_binary(backfill_id) <- field(metadata, :backfill_id),
         window_id when is_binary(window_id) <- field(metadata, :backfill_window_id),
         %BackfillWindow{} = window <-
           Repo.get_by(BackfillWindow,
             workspace_id: workspace_id,
             backfill_id: backfill_id,
             window_id: window_id
           ),
         ^run_id <- BackfillPlan.child_run_id(backfill_id, window_id, window.payload),
         %Backfill{} = backfill <-
           Repo.get_by(Backfill, workspace_id: workspace_id, backfill_id: backfill_id),
         %Backfill{backfill_id: ^backfill_id} <-
           backfill!(workspace_id, backfill.root_run_id) do
      backfill.root_run_id
    else
      _invalid -> invalid!()
    end
  end

  defp recovery_owner!(workspace_id, run_id, intent) do
    with {:ok, {:rerun, source_run_id, opts}} <- Intent.decode(intent),
         metadata <- Keyword.get(opts, :metadata, %{}),
         ^source_run_id <- field(metadata, :resource_recovery_source_run_id),
         ids when is_list(ids) and ids != [] and length(ids) <= 500 <-
           field(metadata, :resource_recovery_candidate_ids),
         candidates <-
           Repo.all(
             from(c in ResourceRecoveryCandidate,
               where:
                 c.workspace_id == ^workspace_id and c.source_run_id == ^source_run_id and
                   c.candidate_id in ^ids
             )
           ),
         true <- length(candidates) == length(Enum.uniq(ids)),
         [{kind, name}] <- Enum.uniq(Enum.map(candidates, &{&1.resource_kind, &1.resource_name})),
         ^run_id <- recovery_id(workspace_id, source_run_id, kind, name, ids) do
      owner!(workspace_id, source_run_id)
    else
      _invalid -> invalid!()
    end
  end

  def recovery_id(workspace_id, source_run_id, kind, name, ids) do
    kind =
      case kind do
        "connection" -> :connection
        "execution_pool" -> :execution_pool
        other -> other
      end

    identity = {workspace_id, source_run_id, kind, name, Enum.sort(ids)}

    hash =
      identity
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "resource-recovery-" <> hash
  end

  def cancelled_error,
    do: Error.new(:conflict, "run cancellation was requested", details: %{reason: :run_cancelled})

  defp invalid!,
    do:
      Repo.rollback(
        Error.new(:conflict, "cancellation ownership cannot be verified",
          details: %{reason: :invalid_cancellation_ownership}
        )
      )

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end

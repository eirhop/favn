defmodule FavnStoragePostgres.RunnerTasks.CleanupReads do
  @moduledoc false
  import Ecto.Query
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.RunOwnership

  @kinds ~w(relation_inspection generation_capabilities generation_marker_read)

  # Only admission under a validated cleanup owner stamps this generation.
  # An old read task or a read kind alone never grants cancellation immunity.
  def stamp(%{run_authority: %{claim_purpose: :cleanup, fencing_token: generation}, run_id: run})
      when is_binary(run), do: generation

  def stamp(_), do: nil

  def authorized?(%{cleanup_fencing_token: generation, run_id: run, task_kind: kind} = task)
      when is_integer(generation) and is_binary(run) and kind in @kinds do
    Repo.exists?(
      from(o in RunOwnership,
        where:
          o.workspace_id == ^task.workspace_id and o.run_id == ^run and
            o.fencing_token == ^generation and o.claim_purpose == "cleanup" and
            is_nil(o.released_at) and o.expires_at > fragment("clock_timestamp()")
      )
    )
  end

  def authorized?(_), do: false

  def exclude_authorized(query) do
    from(t in query,
      where:
        fragment(
          "NOT EXISTS (SELECT 1 FROM favn_control.run_ownerships o WHERE o.workspace_id=? AND o.run_id=? AND o.fencing_token=? AND o.claim_purpose='cleanup' AND o.released_at IS NULL AND o.expires_at>clock_timestamp())",
          t.workspace_id,
          t.run_id,
          t.cleanup_fencing_token
        )
    )
  end
end

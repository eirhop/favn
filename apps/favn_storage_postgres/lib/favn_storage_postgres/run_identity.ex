defmodule FavnStoragePostgres.RunIdentity do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  @spec lock!(String.t(), String.t()) :: :ok
  def lock!(workspace_id, run_id) when is_binary(workspace_id) and is_binary(run_id) do
    SQL.query!(
      Repo,
      """
      SELECT pg_advisory_xact_lock(
        hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0)
      )
      """,
      [workspace_id, run_id]
    )

    :ok
  end
end

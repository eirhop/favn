defmodule FavnStoragePostgres.RuntimeInputKeyInventory do
  @moduledoc """
  Maintains the compact inventory of encryption-key versions used by runtime-input pins.

  Key material remains outside PostgreSQL. Removing an unreferenced inventory row allows
  operators to remove that version from the external keyring without failing readiness.
  """

  alias Ecto.Adapters.SQL

  @doc """
  Removes key versions that are no longer referenced by persisted runtime-input pins.

  The table lock prevents a concurrent pin insert from racing the reference check. Pin
  writes remain blocked only for this single, normally small delete transaction.
  """
  @spec compact(module()) :: {:ok, [pos_integer()]} | {:error, term()}
  def compact(repo) when is_atom(repo) do
    repo.transaction(fn ->
      with {:ok, _result} <-
             SQL.query(repo, "LOCK TABLE favn_control.runtime_input_pins IN SHARE MODE", []),
           {:ok, %{rows: rows}} <-
             SQL.query(
               repo,
               """
               DELETE FROM favn_control.runtime_input_key_versions AS inventory
               WHERE NOT EXISTS (
                 SELECT 1
                 FROM favn_control.runtime_input_pins AS pin
                 WHERE pin.encryption_key_version = inventory.key_version
               )
               RETURNING key_version
               """,
               []
             ) do
        rows
        |> Enum.map(fn [version] -> version end)
        |> Enum.sort()
      else
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end
end

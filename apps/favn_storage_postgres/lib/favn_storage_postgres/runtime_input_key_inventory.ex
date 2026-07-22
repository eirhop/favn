defmodule FavnStoragePostgres.RuntimeInputKeyInventory do
  @moduledoc """
  Maintains the compact inventory of encryption-key versions used by runtime-input pins.

  Key material remains outside PostgreSQL. Removing an unreferenced inventory row allows
  operators to remove that version from the external keyring without failing readiness.
  """

  alias Ecto.Adapters.SQL

  @type entry :: %{
          key_version: pos_integer(),
          first_used_at: DateTime.t(),
          pin_count: non_neg_integer()
        }

  @doc "Returns key-version metadata and pin counts without reading key material."
  @spec list(module()) :: {:ok, [entry()]} | {:error, term()}
  def list(repo) when is_atom(repo) do
    case SQL.query(
           repo,
           """
           SELECT inventory.key_version, inventory.first_used_at, count(pin.run_id)
           FROM favn_control.runtime_input_key_versions AS inventory
           LEFT JOIN favn_control.runtime_input_pins AS pin
             ON pin.encryption_key_version = inventory.key_version
           GROUP BY inventory.key_version, inventory.first_used_at
           ORDER BY inventory.key_version
           """,
           []
         ) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [version, first_used_at, pin_count] ->
           %{key_version: version, first_used_at: first_used_at, pin_count: pin_count}
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Removes only the requested key versions when none of them is still referenced.

  The table lock prevents a concurrent pin insert from racing the reference check. Pin
  writes remain blocked only for this single, normally small delete transaction.
  """
  @spec compact(module(), [pos_integer()]) ::
          {:ok, [pos_integer()]} | {:error, term()}
  def compact(repo, versions)
      when is_atom(repo) and is_list(versions) and versions != [] do
    repo.transaction(fn ->
      with {:ok, _result} <-
             SQL.query(repo, "LOCK TABLE favn_control.runtime_input_pins IN SHARE MODE", []),
           {:ok, referenced_versions} <- referenced_versions(repo, versions),
           :ok <- require_unreferenced(repo, referenced_versions),
           {:ok, %{rows: rows}} <-
             SQL.query(
               repo,
               """
               DELETE FROM favn_control.runtime_input_key_versions AS inventory
               WHERE inventory.key_version = ANY($1)
                 AND
                 NOT EXISTS (
                 SELECT 1
                 FROM favn_control.runtime_input_pins AS pin
                 WHERE pin.encryption_key_version = inventory.key_version
               )
               RETURNING key_version
               """,
               [versions]
             ) do
        rows
        |> Enum.map(fn [version] -> version end)
        |> Enum.sort()
      else
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end

  defp referenced_versions(repo, versions) do
    SQL.query(
      repo,
      """
      SELECT DISTINCT encryption_key_version
      FROM favn_control.runtime_input_pins
      WHERE encryption_key_version = ANY($1)
      ORDER BY encryption_key_version
      """,
      [versions]
    )
    |> case do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [version] -> version end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp require_unreferenced(_repo, []), do: :ok

  defp require_unreferenced(repo, versions) do
    repo.rollback({:runtime_input_key_versions_still_referenced, versions})
  end
end

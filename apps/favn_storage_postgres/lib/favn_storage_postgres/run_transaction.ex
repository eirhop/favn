defmodule FavnStoragePostgres.RunTransaction do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Repo

  # SET LOCAL happens only at the outer boundary. Nested calls cannot renew the
  # server's total transaction budget. Administrative transactions use Repo.
  def transaction(fun, opts \\ []) do
    if Repo.in_transaction?() do
      Repo.transaction(fun, opts)
    else
      Repo.transaction(
        fn ->
          SQL.query!(Repo, "SET LOCAL transaction_timeout = '15s'", [])
          fun.()
        end,
        opts
      )
    end
  end
end

defmodule FavnStorageSqlite.Repo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :favn_storage_sqlite,
    adapter: Ecto.Adapters.SQLite3
end

defmodule FavnStoragePostgres.Repo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :favn_storage_postgres,
    adapter: Ecto.Adapters.Postgres
end

defmodule Favn.Storage.Postgres.Repo do
  @moduledoc """
  Ecto repository used by `Favn.Storage.Adapter.Postgres`.
  """

  use Ecto.Repo,
    otp_app: :favn,
    priv: "priv/favn/storage/postgres",
    adapter: Ecto.Adapters.Postgres
end

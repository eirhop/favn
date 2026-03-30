defmodule Favn.Storage.SQLite.Repo do
  @moduledoc """
  Ecto repository used by `Favn.Storage.Adapter.SQLite`.
  """

  use Ecto.Repo,
    otp_app: :favn,
    priv: "priv/favn/storage/sqlite",
    adapter: Ecto.Adapters.SQLite3
end

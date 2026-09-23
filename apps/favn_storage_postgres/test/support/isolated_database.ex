defmodule FavnStoragePostgres.TestSupport.IsolatedDatabase do
  @moduledoc false

  # A separate renewal pool must observe committed rows. Give each such test its
  # own disposable database rather than pretending two pools share a sandbox.
  def create!(url) do
    uri = URI.parse(url)

    unless String.starts_with?(uri.path || "", "/favn_test"),
      do: raise("isolated tests require a disposable favn_test database URL")

    name = "favn_test_lease_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    options = Ecto.Repo.Supervisor.parse_url(%{uri | path: "/postgres"} |> URI.to_string())
    {:ok, admin} = Postgrex.start_link(options)
    Postgrex.query!(admin, "CREATE DATABASE " <> name, [])
    GenServer.stop(admin)

    ExUnit.Callbacks.on_exit(fn ->
      {:ok, admin} = Postgrex.start_link(options)
      Postgrex.query!(admin, "DROP DATABASE " <> name <> " WITH (FORCE)", [])
      GenServer.stop(admin)
    end)

    %{uri | path: "/" <> name} |> URI.to_string()
  end
end

defmodule FavnStoragePostgres.StorageV2.ManagedIdentityConnectionTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Authentication

  defmodule PasswordProvider do
    def connection_password(options) do
      agent = Keyword.fetch!(options, :agent)

      Agent.get_and_update(agent, fn state ->
        count = state.count + 1
        send(state.owner, {:credential_supplied, count})
        {{:ok, state.password}, %{state | count: count}}
      end)
    end
  end

  test "a real Postgrex pool obtains credentials for initial and replacement connections" do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    parsed = Ecto.Repo.Supervisor.parse_url(url)

    password =
      case Keyword.get(parsed, :password) do
        password when is_binary(password) and password != "" -> password
        _missing -> raise "FAVN_DATABASE_URL must contain a password for this connection test"
      end

    owner = self()

    agent =
      start_supervised!(
        {Agent, fn -> %{owner: owner, password: password, count: 0} end},
        id: :managed_identity_password_provider
      )

    options =
      parsed
      |> Keyword.put(:password, nil)
      |> Keyword.put(:ssl, false)
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(
        :configure,
        {Authentication, :configure_connection, [PasswordProvider, [agent: agent]]}
      )
      |> Keyword.put(:show_sensitive_data_on_connection_error, false)

    refute inspect(options) =~ password

    pool = start_supervised!({Postgrex, options})

    assert_receive {:credential_supplied, 1}, 1_000
    assert %{rows: [[1]]} = Postgrex.query!(pool, "SELECT 1", [])

    assert :ok = DBConnection.disconnect_all(pool, 0)
    assert %{rows: [[1]]} = eventually_query(pool, 50)
    assert_receive {:credential_supplied, replacement_count}, 1_000
    assert replacement_count >= 2
    assert Agent.get(agent, & &1.count) >= 2
  end

  defp eventually_query(pool, attempts) when attempts > 1 do
    case Postgrex.query(pool, "SELECT 1", [], timeout: 1_000) do
      {:ok, result} ->
        result

      {:error, _reason} ->
        Process.sleep(20)
        eventually_query(pool, attempts - 1)
    end
  end

  defp eventually_query(pool, _attempts), do: Postgrex.query!(pool, "SELECT 1", [])
end

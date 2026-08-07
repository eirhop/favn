defmodule FavnStoragePostgres.Bootstrap.Profile do
  @moduledoc false

  @enforce_keys [:purpose, :authentication_mode, :role, :source, :base_env]
  defstruct [:purpose, :authentication_mode, :role, :source, :object_id, :base_env]

  @type purpose :: :bootstrap | :migrator | :runtime
  @type authentication_mode :: :password | :azure_managed_identity
  @type source :: {:url, String.t()} | {:azure, String.t(), pos_integer(), String.t()}

  @type t :: %__MODULE__{
          purpose: purpose(),
          authentication_mode: authentication_mode(),
          role: String.t(),
          source: source(),
          object_id: String.t() | nil,
          base_env: map()
        }

  defimpl Inspect, for: FavnStoragePostgres.Bootstrap.Profile do
    import Inspect.Algebra

    def inspect(profile, opts) do
      concat([
        "#FavnStoragePostgres.Bootstrap.Profile<",
        to_doc(
          %{
            purpose: profile.purpose,
            authentication_mode: profile.authentication_mode,
            role: profile.role,
            object_id_configured?: is_binary(profile.object_id)
          },
          opts
        ),
        ">"
      ])
    end
  end

  @spec connection_env(t(), String.t(), atom()) :: map()
  def connection_env(%__MODULE__{} = profile, database, lifecycle)
      when is_binary(database) and is_atom(lifecycle) do
    profile.base_env
    |> Map.put("FAVN_DATABASE_AUTH_PROFILE", Atom.to_string(lifecycle))
    |> put_connection(profile, database)
  end

  @spec password(t()) :: String.t() | nil
  def password(%__MODULE__{source: {:url, url}}) do
    url
    |> Ecto.Repo.Supervisor.parse_url()
    |> Keyword.get(:password)
  end

  def password(%__MODULE__{}), do: nil

  @spec endpoint(t()) :: {:ok, {String.t(), pos_integer()}} | {:error, :invalid_endpoint}
  def endpoint(%__MODULE__{source: {:url, url}}) do
    options = Ecto.Repo.Supervisor.parse_url(url)

    case {Keyword.get(options, :hostname), Keyword.get(options, :port, 5432)} do
      {hostname, port} when is_binary(hostname) and hostname != "" and is_integer(port) ->
        {:ok, {String.downcase(hostname), port}}

      _invalid ->
        {:error, :invalid_endpoint}
    end
  rescue
    _exception -> {:error, :invalid_endpoint}
  end

  def endpoint(%__MODULE__{source: {:azure, hostname, port, _client_id}}),
    do: {:ok, {String.downcase(hostname), port}}

  @spec database(t()) :: String.t() | nil
  def database(%__MODULE__{source: {:url, url}}) do
    url
    |> Ecto.Repo.Supervisor.parse_url()
    |> Keyword.get(:database)
  end

  def database(%__MODULE__{}), do: nil

  defp put_connection(env, %__MODULE__{source: {:url, url}}, database) do
    env
    |> Map.put("FAVN_DATABASE_AUTH_MODE", "password")
    |> Map.put("FAVN_DATABASE_URL", replace_database(url, database))
  end

  defp put_connection(
         env,
         %__MODULE__{source: {:azure, hostname, port, client_id}, role: role},
         database
       ) do
    env
    |> Map.put("FAVN_DATABASE_AUTH_MODE", "azure_managed_identity")
    |> Map.put("FAVN_DATABASE_HOST", hostname)
    |> Map.put("FAVN_DATABASE_PORT", Integer.to_string(port))
    |> Map.put("FAVN_DATABASE_NAME", database)
    |> Map.put("FAVN_DATABASE_USERNAME", role)
    |> Map.put("FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID", client_id)
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/" <> database})
  end
end

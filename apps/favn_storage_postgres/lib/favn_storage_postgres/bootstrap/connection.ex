defmodule FavnStoragePostgres.Bootstrap.Connection do
  @moduledoc false

  alias FavnStoragePostgres.Authentication
  alias FavnStoragePostgres.Bootstrap.{Config, Profile}
  alias FavnStoragePostgres.Config, as: DatabaseConfig
  alias FavnStoragePostgres.Repo

  @type error_code ::
          :authentication_rejected
          | :authentication_unavailable
          | :database_access_rejected
          | :database_missing
          | :dependency_unavailable
          | :invalid_database_configuration
          | :server_unreachable

  @spec with_raw(Config.t(), Profile.t(), String.t(), atom(), (pid() -> result)) ::
          result | {:error, error_code()}
        when result: term()
  def with_raw(config, profile, database, lifecycle, function)
      when is_function(function, 1) do
    with {:ok, connection_config} <- connection_config(config, profile, database, lifecycle),
         :ok <- ensure_dependencies(connection_config.authentication),
         {:ok, authentication_state} <- start_authentication(connection_config.authentication) do
      try do
        with :ok <- authentication_ready(connection_config.authentication),
             {:ok, connection} <- start_connection(connection_config, profile) do
          try do
            function.(connection)
          after
            stop_process(connection)
          end
        end
      after
        stop_authentication(authentication_state)
      end
    end
  end

  @spec with_repo(Config.t(), Profile.t(), String.t(), atom(), (module() -> result)) ::
          result | {:error, error_code()}
        when result: term()
  def with_repo(config, profile, database, lifecycle, function)
      when is_function(function, 1) do
    with {:ok, connection_config} <- connection_config(config, profile, database, lifecycle),
         :ok <- ensure_dependencies(connection_config.authentication),
         {:ok, authentication_state} <- start_authentication(connection_config.authentication) do
      try do
        with :ok <- authentication_ready(connection_config.authentication),
             {:ok, repo} <-
               start_repo(connection_config.repo_options, connection_config.authentication) do
          try do
            case verify_repo_connection(connection_config.authentication) do
              :ok -> function.(Repo)
              {:error, code} -> {:error, code}
            end
          after
            stop_process(repo)
          end
        end
      after
        stop_authentication(authentication_state)
      end
    end
  end

  defp connection_config(config, profile, database, lifecycle) do
    env = Config.connection_env(config, profile, database, lifecycle)

    case DatabaseConfig.connection_config_from_env(env) do
      {:ok, connection_config} -> {:ok, connection_config}
      {:error, _reason} -> {:error, :invalid_database_configuration}
    end
  end

  defp ensure_dependencies(authentication) do
    with {:ok, _started} <- Application.ensure_all_started(:ecto_sql),
         {:ok, _started} <- Application.ensure_all_started(:postgrex),
         {:ok, applications} <- Authentication.applications(authentication) do
      Enum.reduce_while(applications, :ok, fn application, :ok ->
        case Application.ensure_all_started(application) do
          {:ok, _started} -> {:cont, :ok}
          {:error, _reason} -> {:halt, {:error, :dependency_unavailable}}
        end
      end)
    else
      {:error, _reason} -> {:error, :dependency_unavailable}
    end
  end

  defp start_authentication(authentication) do
    with {:ok, child_specs} <- Authentication.child_specs(authentication) do
      case child_specs do
        [] ->
          {:ok, nil}

        specs ->
          case Supervisor.start_link(specs, strategy: :one_for_one) do
            {:ok, pid} -> {:ok, pid}
            {:error, _reason} -> {:error, :authentication_unavailable}
          end
      end
    else
      {:error, _reason} -> {:error, :authentication_unavailable}
    end
  end

  defp authentication_ready(:password), do: :ok

  defp authentication_ready(authentication) do
    case Authentication.status(authentication) do
      %{lifecycle_ready?: true} -> :ok
      _status -> {:error, :authentication_unavailable}
    end
  end

  defp start_connection(connection_config, profile) do
    options =
      connection_config.notification_options
      |> Keyword.drop([:auto_reconnect, :sync_connect])
      |> Keyword.put(:backoff_type, :stop)

    case Postgrex.start_link(options) do
      {:ok, connection} ->
        try do
          :ok = DatabaseConfig.after_connect(connection, "bootstrap:#{profile.purpose}")
          {:ok, connection}
        rescue
          exception ->
            stop_process(connection)
            classify_connection_error(exception, connection_config.authentication)
        end

      {:error, reason} ->
        classify_connection_error(reason, connection_config.authentication)
    end
  end

  defp start_repo(options, authentication) do
    case Repo.start_link(options) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, _pid}} -> {:error, :dependency_unavailable}
      {:error, reason} -> classify_connection_error(reason, authentication)
    end
  end

  defp verify_repo_connection(authentication) do
    case Ecto.Adapters.SQL.query(Repo, "SELECT 1", []) do
      {:ok, _result} -> :ok
      {:error, reason} -> classify_connection_error(reason, authentication)
    end
  catch
    :exit, reason -> classify_connection_error(reason, authentication)
  end

  defp classify_connection_error(reason, authentication) do
    case authentication_failure(authentication) do
      nil -> classify_database_error(reason)
      code -> {:error, code}
    end
  end

  defp classify_database_error(%Postgrex.Error{postgres: %{code: code}})
       when code in [:invalid_authorization_specification, :invalid_password] do
    {:error, :authentication_rejected}
  end

  defp classify_database_error(%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}),
    do: {:error, :database_missing}

  defp classify_database_error(%Postgrex.Error{postgres: %{code: :insufficient_privilege}}),
    do: {:error, :database_access_rejected}

  defp classify_database_error(%DBConnection.ConnectionError{}),
    do: {:error, :server_unreachable}

  defp classify_database_error(%Postgrex.Error{}), do: {:error, :server_unreachable}
  defp classify_database_error(_reason), do: {:error, :server_unreachable}

  defp authentication_failure(:password), do: nil

  defp authentication_failure(authentication) do
    case Authentication.status(authentication) do
      %{last_failure_class: class}
      when class in [
             :token_timeout,
             :identity_unavailable,
             :provider_unavailable,
             :insufficient_validity
           ] ->
        :authentication_unavailable

      %{last_failure_class: class} when class in [:identity_rejected, :invalid_config] ->
        :authentication_rejected

      _status ->
        nil
    end
  end

  defp stop_authentication(nil), do: :ok
  defp stop_authentication(pid), do: stop_process(pid)

  defp stop_process(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end

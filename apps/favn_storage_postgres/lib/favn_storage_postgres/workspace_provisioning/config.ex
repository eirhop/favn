defmodule FavnStoragePostgres.WorkspaceProvisioning.Config do
  @moduledoc false

  alias FavnStoragePostgres.AdminSecret
  alias FavnOrchestrator.WorkspaceProvisioning

  @max_config_bytes 64 * 1_024

  @spec load(map()) :: {:ok, map()} | {:error, atom()}
  def load(env) when is_map(env) do
    with {:ok, path} <- required(env, "FAVN_WORKSPACE_PROVISIONING_CONFIG_FILE"),
         {:ok, encoded} <- read_bounded(path),
         {:ok, config} <- Jason.decode(encoded),
         :ok <- validate_contract(config),
         {:ok, config} <- attach_password(config, env),
         :ok <- WorkspaceProvisioning.validate(config) do
      {:ok, config}
    else
      {:error, reason} when is_atom(reason) -> {:error, reason}
      {:error, _reason} -> {:error, :invalid_workspace_provisioning_config}
    end
  end

  @spec workspace_id(map()) :: {:ok, String.t()} | {:error, atom()}
  def workspace_id(env) when is_map(env) do
    required(env, "FAVN_WORKSPACE_ID")
  end

  defp read_bounded(path) do
    with {:ok, stat} <- File.stat(path),
         true <- stat.type == :regular,
         true <- stat.size in 1..@max_config_bytes,
         {:ok, encoded} <- File.read(path) do
      {:ok, encoded}
    else
      _invalid -> {:error, :invalid_workspace_provisioning_config}
    end
  end

  defp validate_contract(%{"contract_version" => 1, "administrator" => administrator})
       when is_map(administrator),
       do: :ok

  defp validate_contract(_config), do: {:error, :invalid_workspace_provisioning_config}

  defp attach_password(%{"administrator" => %{"mode" => "entra"}} = config, _env),
    do: {:ok, config}

  defp attach_password(
         %{"administrator" => %{"mode" => "password"} = administrator} = config,
         env
       ) do
    opts =
      case Map.get(env, "FAVN_WORKSPACE_ADMIN_PASSWORD_FILE") do
        nil -> []
        path -> [password_file: path]
      end

    with {:ok, password} <- AdminSecret.read(opts, "Initial administrator password") do
      {:ok, put_in(config, ["administrator"], Map.put(administrator, "password", password))}
    else
      {:error, _message} -> {:error, :password_input_failed}
    end
  end

  defp attach_password(_config, _env), do: {:error, :invalid_workspace_provisioning_config}

  defp required(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and byte_size(value) in 1..1_024 -> {:ok, value}
      _missing -> {:error, :missing_or_invalid_environment}
    end
  end
end

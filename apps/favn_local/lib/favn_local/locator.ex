defmodule FavnLocal.Locator do
  @moduledoc false

  alias FavnLocal.Config
  alias FavnLocal.Distribution

  @state_schema 1

  @type state :: %{
          node: node(),
          cookie: String.t(),
          orchestrator_url: String.t(),
          service_token: String.t(),
          workspace_id: String.t(),
          runner_release_id: String.t()
        }

  @spec write(Config.t(), String.t()) :: :ok | {:error, term()}
  def write(%Config{} = config, runner_release_id) when is_binary(runner_release_id) do
    with :ok <- reject_legacy_state(config.root_dir),
         :ok <- File.mkdir_p(state_dir(config.root_dir)),
         :ok <- write_secret(secret_path(config.root_dir), config),
         :ok <-
           atomic_write(
             state_path(config.root_dir),
             JSON.encode!(%{
               "schema_version" => @state_schema,
               "project_root" => config.root_dir,
               "operator_node" => Atom.to_string(config.operator_node),
               "orchestrator_url" => "http://127.0.0.1:#{config.orchestrator_port}",
               "workspace_id" => config.workspace_id,
               "runner_release_id" => runner_release_id
             })
           ) do
      :ok
    end
  end

  @spec delete(Path.t()) :: :ok
  def delete(root_dir) do
    _ = File.rm(state_path(root_dir))
    :ok
  end

  @spec read(Path.t()) :: {:ok, state()} | {:error, term()}
  def read(root_dir) do
    with {:ok, state_bytes} <- File.read(state_path(root_dir)),
         {:ok, secret_bytes} <- File.read(secret_path(root_dir)),
         {:ok,
          %{
            "schema_version" => @state_schema,
            "project_root" => project_root,
            "operator_node" => operator_node,
            "orchestrator_url" => orchestrator_url,
            "workspace_id" => workspace_id,
            "runner_release_id" => runner_release_id
          }} <- JSON.decode(state_bytes),
         true <- Path.expand(project_root) == Path.expand(root_dir),
         {:ok, %{"cookie" => cookie, "service_token" => service_token}} <-
           JSON.decode(secret_bytes) do
      {:ok,
       %{
         node: String.to_atom(operator_node),
         cookie: cookie,
         orchestrator_url: orchestrator_url,
         service_token: service_token,
         workspace_id: workspace_id,
         runner_release_id: runner_release_id
       }}
    else
      {:error, :enoent} -> {:error, :not_running}
      false -> {:error, :stale_locator}
      _invalid -> {:error, :invalid_locator}
    end
  end

  @spec connect(Path.t()) :: {:ok, state()} | {:error, term()}
  def connect(root_dir) do
    with {:ok, state} <- read(root_dir),
         :ok <- ensure_client_node(state.cookie),
         :pong <- Node.ping(state.node) do
      {:ok, state}
    else
      :pang -> {:error, :not_running}
      {:error, _reason} = error -> error
    end
  end

  @spec local_client_options(Path.t()) :: {:ok, keyword()} | {:error, term()}
  def local_client_options(root_dir) do
    with {:ok, state} <- read(root_dir) do
      {:ok,
       [
         orchestrator_url: state.orchestrator_url,
         service_token: state.service_token,
         workspace_id: state.workspace_id
       ]}
    end
  end

  defp ensure_client_node(cookie) do
    if Node.alive?() do
      Node.set_cookie(String.to_atom(cookie))
      :ok
    else
      suffix = System.unique_integer([:positive, :monotonic])
      name = String.to_atom("favn_local_client_#{suffix}@127.0.0.1")

      case Distribution.start(name, cookie) do
        :ok ->
          :ok

        {:error, reason} ->
          {:error, {:client_node_start_failed, reason}}
      end
    end
  end

  defp reject_legacy_state(root_dir) do
    favn_dir = Path.join(root_dir, ".favn")

    legacy_entries =
      ~w(
        build
        compose
        history
        install
        logs
        maintenance.json
        manifests
        runner.json
        runtime.json
        secrets.json
      )
      |> Enum.map(&Path.join(favn_dir, &1))
      |> Enum.filter(&File.exists?/1)

    if legacy_entries == [],
      do: :ok,
      else: {:error, {:legacy_local_state, Path.join(root_dir, ".favn")}}
  end

  defp write_secret(path, config) do
    with :ok <-
           atomic_write(
             path,
             JSON.encode!(%{
               "cookie" => config.distribution_cookie,
               "service_token" => config.service_token,
               "view_username" => "admin",
               "view_password" => config.bootstrap_password
             })
           ),
         :ok <- File.chmod(path, 0o600) do
      :ok
    end
  end

  defp atomic_write(path, bytes) do
    temporary = path <> ".tmp-#{System.unique_integer([:positive])}"

    with :ok <- File.write(temporary, bytes <> "\n"),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temporary)
        {:error, reason}
    end
  end

  defp state_dir(root_dir), do: Path.join([Path.expand(root_dir), ".favn", "local"])
  defp state_path(root_dir), do: Path.join(state_dir(root_dir), "state.json")
  defp secret_path(root_dir), do: Path.join(state_dir(root_dir), "credentials.json")
end

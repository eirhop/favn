defmodule FavnOrchestrator.RuntimeConfig do
  @moduledoc """
  Normalized runtime dependency contract for the orchestrator process tree.

  Application env remains the boot-time input for deployment and local-dev
  ergonomics. Once the application starts, hot runtime paths read this explicit
  struct from the supervised process instead of repeatedly consulting mutable
  global env.
  """

  use GenServer

  alias FavnOrchestrator.API.ManifestPublication.Config, as: ManifestPublicationConfig

  @default_auth_session_ttl_seconds 43_200
  @max_auth_session_ttl_seconds 2_592_000

  @type t :: %__MODULE__{
          runner_client: module() | nil,
          runner_client_opts: keyword(),
          workspace_ids: [String.t()],
          api_server: keyword(),
          scheduler: keyword(),
          log_redaction_policy: term(),
          instance_id: String.t(),
          http_server: map(),
          shutdown_drain_timeout_ms: pos_integer(),
          manifest_publication: ManifestPublicationConfig.t(),
          auth_session_ttl_seconds: pos_integer()
        }
  @type error :: {:invalid_runtime_config, {atom(), term()}}

  defstruct runner_client: nil,
            runner_client_opts: [],
            workspace_ids: [],
            api_server: [],
            scheduler: [],
            log_redaction_policy: nil,
            instance_id: "local",
            http_server: %{
              max_connections: 1_024,
              request_timeout_ms: 30_000,
              idle_timeout_ms: 60_000,
              body_limit_bytes: 1_048_576
            },
            shutdown_drain_timeout_ms: 120_000,
            manifest_publication: %ManifestPublicationConfig{
              compressed_limit_bytes: 8 * 1_024 * 1_024,
              decompressed_limit_bytes: 32 * 1_024 * 1_024
            },
            auth_session_ttl_seconds: @default_auth_session_ttl_seconds

  @doc """
  Starts the runtime config holder.
  """
  @spec start_link(t() | keyword()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = config) do
    GenServer.start_link(__MODULE__, {config, __MODULE__}, name: __MODULE__)
  end

  def start_link(opts) when is_list(opts) do
    config =
      case Keyword.fetch(opts, :config) do
        {:ok, config} -> normalize!(config)
        :error -> from_app_env()
      end

    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, {config, name}, name: name)
  end

  @doc """
  Returns the active normalized runtime config.

  If the orchestrator supervision tree is not running, this falls back to a fresh
  normalization from application env so unit tests and standalone helper calls
  keep their existing ergonomics.
  """
  @spec current(atom()) :: t()
  def current(name \\ __MODULE__) do
    if dynamic_env_override?(name) do
      from_app_env()
    else
      case :persistent_term.get(persistent_key(name), :missing) do
        :missing -> from_app_env()
        %__MODULE__{} = config -> config
      end
    end
  end

  @doc """
  Builds the runtime dependency contract from boot-time application env.
  """
  @spec from_app_env() :: t()
  def from_app_env do
    normalize!(
      runner_client: Application.get_env(:favn_orchestrator, :runner_client, nil),
      runner_client_opts: Application.get_env(:favn_orchestrator, :runner_client_opts, []),
      workspace_ids: Application.get_env(:favn_orchestrator, :workspace_ids, []),
      api_server: Application.get_env(:favn_orchestrator, :api_server, []),
      scheduler: Application.get_env(:favn_orchestrator, :scheduler, []),
      log_redaction_policy: Application.get_env(:favn_orchestrator, :log_redaction_policy),
      instance_id: Application.get_env(:favn_orchestrator, :instance_id, "local"),
      http_server: Application.get_env(:favn_orchestrator, :http_server, %{}),
      shutdown_drain_timeout_ms:
        Application.get_env(:favn_orchestrator, :shutdown_drain_timeout_ms, 120_000),
      manifest_publication: Application.get_env(:favn_orchestrator, :manifest_publication, []),
      auth_session_ttl_seconds:
        Application.get_env(
          :favn_orchestrator,
          :auth_session_ttl_seconds,
          @default_auth_session_ttl_seconds
        )
    )
  end

  @doc """
  Normalizes runtime dependency options into a stable struct.
  """
  @spec normalize(keyword() | map() | t()) :: {:ok, t()} | {:error, error()}
  def normalize(%__MODULE__{} = config), do: {:ok, config}

  def normalize(attrs) when is_map(attrs) do
    attrs
    |> Map.to_list()
    |> normalize()
  end

  def normalize(attrs) when is_list(attrs) do
    runner_client = Keyword.get(attrs, :runner_client, nil)
    runner_client_opts = Keyword.get(attrs, :runner_client_opts, [])
    workspace_ids = Keyword.get(attrs, :workspace_ids, [])
    api_server = Keyword.get(attrs, :api_server, [])
    scheduler = Keyword.get(attrs, :scheduler, [])
    instance_id = Keyword.get(attrs, :instance_id, "local")
    http_server = normalize_http_server(Keyword.get(attrs, :http_server, %{}))
    shutdown_drain_timeout_ms = Keyword.get(attrs, :shutdown_drain_timeout_ms, 120_000)
    manifest_publication = Keyword.get(attrs, :manifest_publication, [])

    auth_session_ttl_seconds =
      Keyword.get(attrs, :auth_session_ttl_seconds, @default_auth_session_ttl_seconds)

    with :ok <- validate_module_or_nil(:runner_client, runner_client),
         {:ok, runner_client_opts} <- validate_keyword(:runner_client_opts, runner_client_opts),
         :ok <- validate_workspace_ids(workspace_ids),
         {:ok, api_server} <- validate_keyword(:api_server, api_server),
         {:ok, scheduler} <- validate_keyword(:scheduler, scheduler),
         :ok <- validate_instance_id(instance_id),
         :ok <- validate_http_server(http_server),
         :ok <- validate_positive_integer(:shutdown_drain_timeout_ms, shutdown_drain_timeout_ms),
         {:ok, manifest_publication} <- normalize_manifest_publication(manifest_publication),
         :ok <- validate_auth_session_ttl(auth_session_ttl_seconds) do
      {:ok,
       %__MODULE__{
         runner_client: runner_client,
         runner_client_opts: runner_client_opts,
         workspace_ids: workspace_ids,
         api_server: api_server,
         scheduler: scheduler,
         log_redaction_policy: Keyword.get(attrs, :log_redaction_policy),
         instance_id: instance_id,
         http_server: http_server,
         shutdown_drain_timeout_ms: shutdown_drain_timeout_ms,
         manifest_publication: manifest_publication,
         auth_session_ttl_seconds: auth_session_ttl_seconds
       }}
    end
  end

  @doc """
  Normalizes runtime dependency options or raises on invalid boot config.
  """
  @spec normalize!(keyword() | map() | t()) :: t()
  def normalize!(attrs) do
    case normalize(attrs) do
      {:ok, config} ->
        config

      {:error, reason} ->
        raise ArgumentError, "invalid orchestrator runtime config: #{inspect(reason)}"
    end
  end

  @doc "Returns the boot-frozen identity of this control-plane node."
  @spec instance_id() :: String.t()
  def instance_id, do: current().instance_id

  @doc "Returns the boot-frozen HTTP server limits."
  @spec http_server() :: map()
  def http_server, do: current().http_server

  @doc "Returns the boot-frozen graceful drain budget in milliseconds."
  @spec shutdown_drain_timeout_ms() :: pos_integer()
  def shutdown_drain_timeout_ms, do: current().shutdown_drain_timeout_ms

  @doc "Returns the boot-frozen manifest publication limits."
  @spec manifest_publication() :: ManifestPublicationConfig.t()
  def manifest_publication, do: current().manifest_publication

  @doc "Returns the boot-frozen browser authentication session lifetime."
  @spec auth_session_ttl_seconds() :: pos_integer()
  def auth_session_ttl_seconds, do: current().auth_session_ttl_seconds

  @doc "Returns the boot-frozen configured workspace identities."
  @spec workspace_ids() :: [String.t()]
  def workspace_ids, do: current().workspace_ids

  @doc "Returns the boot-frozen private API server options."
  @spec api_server() :: keyword()
  def api_server, do: current().api_server

  @doc "Returns the boot-frozen scheduler options."
  @spec scheduler() :: keyword()
  def scheduler, do: current().scheduler

  @impl true
  def init({%__MODULE__{} = config, name}) do
    :persistent_term.put(persistent_key(name), config)
    {:ok, %{config: config, name: name}}
  end

  @impl true
  def handle_call(:current, _from, state), do: {:reply, state.config, state}

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase(persistent_key(state.name))
    :ok
  end

  defp dynamic_env_override?(__MODULE__) do
    Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?, false) == true
  end

  defp dynamic_env_override?(_name), do: false

  defp persistent_key(name), do: {__MODULE__, name}

  defp validate_module_or_nil(_field, nil), do: :ok
  defp validate_module_or_nil(field, module), do: validate_module(field, module)

  defp validate_module(_field, module) when is_atom(module), do: :ok
  defp validate_module(field, value), do: {:error, {:invalid_runtime_config, {field, value}}}

  defp validate_keyword(field, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      {:ok, opts}
    else
      {:error, {:invalid_runtime_config, {field, opts}}}
    end
  end

  defp validate_keyword(field, value), do: {:error, {:invalid_runtime_config, {field, value}}}

  defp validate_workspace_ids(workspace_ids) when is_list(workspace_ids) do
    if Enum.all?(workspace_ids, &(is_binary(&1) and byte_size(&1) in 1..255)) and
         length(workspace_ids) == length(Enum.uniq(workspace_ids)) do
      :ok
    else
      {:error, {:invalid_runtime_config, {:workspace_ids, :invalid}}}
    end
  end

  defp validate_workspace_ids(_value),
    do: {:error, {:invalid_runtime_config, {:workspace_ids, :invalid}}}

  defp validate_instance_id(instance_id)
       when is_binary(instance_id) and byte_size(instance_id) in 1..160,
       do: :ok

  defp validate_instance_id(value),
    do: {:error, {:invalid_runtime_config, {:instance_id, value}}}

  defp normalize_http_server(http_server) when is_map(http_server) do
    Map.merge(
      %{
        max_connections: 1_024,
        request_timeout_ms: 30_000,
        idle_timeout_ms: 60_000,
        body_limit_bytes: 1_048_576
      },
      http_server
    )
  end

  defp normalize_http_server(value), do: value

  defp validate_http_server(http_server) when is_map(http_server) do
    [:max_connections, :request_timeout_ms, :idle_timeout_ms, :body_limit_bytes]
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case validate_positive_integer(key, Map.get(http_server, key)) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_http_server(value),
    do: {:error, {:invalid_runtime_config, {:http_server, value}}}

  defp validate_positive_integer(_field, value) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_integer(field, value),
    do: {:error, {:invalid_runtime_config, {field, value}}}

  defp normalize_manifest_publication(%ManifestPublicationConfig{} = config), do: {:ok, config}

  defp normalize_manifest_publication(config) do
    case ManifestPublicationConfig.new(config) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, reason} -> {:error, {:invalid_runtime_config, {:manifest_publication, reason}}}
    end
  end

  defp validate_auth_session_ttl(ttl)
       when is_integer(ttl) and ttl in 1..@max_auth_session_ttl_seconds,
       do: :ok

  defp validate_auth_session_ttl(value),
    do: {:error, {:invalid_runtime_config, {:auth_session_ttl_seconds, value}}}
end

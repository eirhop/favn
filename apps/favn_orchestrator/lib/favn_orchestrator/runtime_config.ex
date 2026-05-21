defmodule FavnOrchestrator.RuntimeConfig do
  @moduledoc """
  Normalized runtime dependency contract for the orchestrator process tree.

  Application env remains the boot-time input for deployment and local-dev
  ergonomics. Once the application starts, hot runtime paths read this explicit
  struct from the supervised process instead of repeatedly consulting mutable
  global env.
  """

  use GenServer

  alias FavnOrchestrator.Storage.Adapter.Memory

  @type t :: %__MODULE__{
          runner_client: module() | nil,
          runner_client_opts: keyword(),
          storage_adapter: module(),
          storage_adapter_opts: keyword(),
          log_redaction_policy: term()
        }

  defstruct runner_client: nil,
            runner_client_opts: [],
            storage_adapter: Memory,
            storage_adapter_opts: [],
            log_redaction_policy: nil

  @doc """
  Starts the runtime config holder.
  """
  @spec start_link(t() | keyword()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  def start_link(opts) when is_list(opts) do
    config = Keyword.get(opts, :config, from_app_env())
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, config, name: name)
  end

  @doc """
  Returns the active normalized runtime config.

  If the orchestrator supervision tree is not running, this falls back to a fresh
  normalization from application env so unit tests and standalone helper calls
  keep their existing ergonomics.
  """
  @spec current(GenServer.server()) :: t()
  def current(name \\ __MODULE__) do
    if dynamic_env_override?(name) do
      from_app_env()
    else
      case Process.whereis(name) do
        nil -> from_app_env()
        _pid -> GenServer.call(name, :current)
      end
    end
  end

  @doc """
  Builds the runtime dependency contract from boot-time application env.
  """
  @spec from_app_env() :: t()
  def from_app_env do
    normalize(
      runner_client: Application.get_env(:favn_orchestrator, :runner_client, nil),
      runner_client_opts: Application.get_env(:favn_orchestrator, :runner_client_opts, []),
      storage_adapter: Application.get_env(:favn_orchestrator, :storage_adapter, Memory),
      storage_adapter_opts: Application.get_env(:favn_orchestrator, :storage_adapter_opts, []),
      log_redaction_policy: Application.get_env(:favn_orchestrator, :log_redaction_policy)
    )
  end

  @doc """
  Normalizes runtime dependency options into a stable struct.
  """
  @spec normalize(keyword() | map() | t()) :: t()
  def normalize(%__MODULE__{} = config), do: config

  def normalize(attrs) when is_map(attrs) do
    attrs
    |> Map.to_list()
    |> normalize()
  end

  def normalize(attrs) when is_list(attrs) do
    %__MODULE__{
      runner_client: Keyword.get(attrs, :runner_client, nil),
      runner_client_opts: normalize_keyword(Keyword.get(attrs, :runner_client_opts, [])),
      storage_adapter: Keyword.get(attrs, :storage_adapter, Memory),
      storage_adapter_opts: normalize_keyword(Keyword.get(attrs, :storage_adapter_opts, [])),
      log_redaction_policy: Keyword.get(attrs, :log_redaction_policy)
    }
  end

  @impl true
  def init(%__MODULE__{} = config), do: {:ok, config}

  @impl true
  def handle_call(:current, _from, %__MODULE__{} = config), do: {:reply, config, config}

  defp dynamic_env_override?(__MODULE__) do
    Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?, false) == true
  end

  defp dynamic_env_override?(_name), do: false

  defp normalize_keyword(opts) when is_list(opts), do: opts
  defp normalize_keyword(_opts), do: []
end

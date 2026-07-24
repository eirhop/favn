defmodule FavnLocal do
  @moduledoc """
  Docker-free source-development lifecycle for Favn consumers.

  Public users normally invoke this boundary through `mix favn.dev`,
  `mix favn.reload`, and `mix favn.stop`.
  """

  alias FavnLocal.Config
  alias FavnLocal.Distribution
  alias FavnLocal.Lifecycle
  alias FavnLocal.Locator
  alias FavnLocal.Preflight
  alias FavnLocal.Publication

  @type progress_event ::
          {:configuration_loaded,
           %{view_url: String.t(), orchestrator_url: String.t(), workspace_id: String.t()}}
          | :postgres_ready
          | {:manifest_built, %{manifest_version_id: String.t()}}
          | {:orchestrator_ready, %{url: String.t()}}
          | {:view_ready, %{url: String.t()}}
          | :runner_starting

  @type progress_fun :: (progress_event() -> term())

  @spec dev(keyword()) :: {:ok, map()} | {:error, term()}
  def dev(opts \\ []) when is_list(opts) do
    result =
      with {:ok, progress_fun} <- progress_fun(opts),
           {:ok, config} <- Config.load(opts),
           :ok <-
             notify_progress(
               progress_fun,
               {:configuration_loaded,
                %{
                  view_url: view_url(config),
                  orchestrator_url: orchestrator_url(config),
                  workspace_id: config.workspace_id
                }}
             ),
           :ok <- start_operator_node(config),
           :ok <- Config.apply(config),
           :ok <- Preflight.run(config),
           :ok <- notify_progress(progress_fun, :postgres_ready),
           {:ok, publication} <- Publication.build(config.runner_release_id),
           :ok <-
             notify_progress(
               progress_fun,
               {:manifest_built, %{manifest_version_id: publication.version.manifest_version_id}}
             ),
           {:ok, _applications} <- Application.ensure_all_started(:favn_orchestrator),
           :ok <-
             notify_progress(
               progress_fun,
               {:orchestrator_ready, %{url: orchestrator_url(config)}}
             ),
           {:ok, _applications} <- Application.ensure_all_started(:favn_view),
           :ok <- notify_progress(progress_fun, {:view_ready, %{url: view_url(config)}}),
           :ok <- notify_progress(progress_fun, :runner_starting),
           {:ok, supervisor} <-
             FavnLocal.Supervisor.start_link(config: config, publication: publication) do
        Process.unlink(supervisor)
        await_startup(supervisor, Keyword.get(opts, :startup_timeout_ms, 60_000))
      end

    if match?({:error, _reason}, result) do
      Config.clear_source_development_auth()
    end

    result
  end

  @spec reload(keyword()) :: {:ok, map()} | {:error, term()}
  def reload(opts \\ []) when is_list(opts) do
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()
    release_id = "rr_" <> random_hex(32)

    with {:ok, publication} <- Publication.build(release_id),
         {:ok, locator} <- Locator.connect(root_dir) do
      :erpc.call(
        locator.node,
        Lifecycle,
        :reload,
        [publication, release_id, Keyword.get(opts, :reload_timeout_ms, 60_000)],
        Keyword.get(opts, :reload_timeout_ms, 60_000) + 1_000
      )
    end
  catch
    :error, reason -> {:error, {:reload_rpc_failed, reason}}
    :exit, reason -> {:error, {:reload_rpc_failed, reason}}
  end

  @spec stop(keyword()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts) do
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()

    case Locator.connect(root_dir) do
      {:ok, locator} ->
        timeout = Keyword.get(opts, :stop_timeout_ms, 60_000)
        :erpc.call(locator.node, Lifecycle, :stop, [timeout], timeout + 1_000)

      {:error, reason} when reason in [:not_running, :stale_locator] ->
        Locator.delete(root_dir)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  catch
    :error, reason -> {:error, {:stop_rpc_failed, reason}}
    :exit, reason -> {:error, {:stop_rpc_failed, reason}}
  end

  @spec await_shutdown(pid()) :: :ok
  def await_shutdown(supervisor) when is_pid(supervisor) do
    monitor = Process.monitor(supervisor)

    receive do
      {:DOWN, ^monitor, :process, ^supervisor, _reason} -> :ok
    end
  end

  defp await_startup(supervisor, timeout_ms) do
    case Lifecycle.await_ready(timeout_ms) do
      {:ok, summary} ->
        {:ok, Map.put(summary, :supervisor, supervisor)}

      {:error, reason} ->
        _ = Lifecycle.stop()
        {:error, reason}
    end
  end

  defp start_operator_node(config) do
    if Node.alive?() do
      {:error, {:node_already_running, node()}}
    else
      case Distribution.start(config.operator_node, config.distribution_cookie) do
        :ok ->
          :ok

        {:error, reason} ->
          {:error, {:operator_node_start_failed, reason}}
      end
    end
  end

  defp progress_fun(opts) do
    case Keyword.get(opts, :progress_fun) do
      nil -> {:ok, fn _event -> :ok end}
      fun when is_function(fun, 1) -> {:ok, fun}
      _invalid -> {:error, {:invalid_dev_config, :progress_fun}}
    end
  end

  defp notify_progress(progress_fun, event) do
    _ = progress_fun.(event)
    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp view_url(config), do: "http://127.0.0.1:#{config.view_port}"
  defp orchestrator_url(config), do: "http://127.0.0.1:#{config.orchestrator_port}"

  defp random_hex(bytes), do: bytes |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
end

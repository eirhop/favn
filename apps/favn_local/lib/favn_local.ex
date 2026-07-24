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

  @spec dev(keyword()) :: {:ok, map()} | {:error, term()}
  def dev(opts \\ []) when is_list(opts) do
    with {:ok, config} <- Config.load(opts),
         :ok <- start_operator_node(config),
         :ok <- Config.apply(config),
         :ok <- Preflight.run(config),
         {:ok, publication} <- Publication.build(config.runner_release_id),
         {:ok, _applications} <- Application.ensure_all_started(:favn_orchestrator),
         {:ok, _applications} <- Application.ensure_all_started(:favn_view),
         {:ok, supervisor} <-
           FavnLocal.Supervisor.start_link(config: config, publication: publication) do
      Process.unlink(supervisor)
      await_startup(supervisor, Keyword.get(opts, :startup_timeout_ms, 60_000))
    end
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

  defp random_hex(bytes), do: bytes |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
end

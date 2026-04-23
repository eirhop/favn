defmodule Favn.Dev.Reload do
  @moduledoc """
  Reload workflow for the running local stack.

  Reload semantics:
  - refuse when stack is not running
  - refuse when in-flight runs exist
  - recompile and rebuild manifest
  - restart runner and re-register manifest in live runner
  - publish and activate manifest in orchestrator without restarting orchestrator
  """

  alias Favn.Dev.Config
  alias Favn.Dev.Lock
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RunnerControl
  alias Favn.Dev.RuntimeLaunch
  alias Favn.Dev.RuntimeWorkspace
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @type reload_opt :: [root_dir: Path.t()]

  @spec run(reload_opt()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- ensure_running(opts),
         {:ok, runtime, installed_runtime, secrets} <- read_runtime_snapshot(opts),
         :ok <- ensure_no_in_flight_runs(runtime, secrets, opts),
         :ok <- compile_project(),
         {:ok, build} <- FavnAuthoring.build_manifest(),
         {:ok, version} <- FavnAuthoring.pin_manifest_version(build.manifest),
         {:ok, runtime_after_restart} <- restart_runner(runtime, installed_runtime, secrets, opts),
         :ok <- register_manifest_in_runner(version, runtime_after_restart, secrets),
         :ok <- write_manifest_cache(version, opts),
         {:ok, _published} <- publish_manifest(version, runtime_after_restart, secrets, opts),
         {:ok, _activated} <- activate_manifest(version, runtime_after_restart, secrets, opts),
         :ok <- update_runtime_manifest(version.manifest_version_id, opts) do
      IO.puts("Favn manifest reloaded: #{version.manifest_version_id}")
      :ok
    end
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      :stopped -> {:error, :stack_not_running}
      _ -> {:error, :stack_not_running}
    end
  end

  defp read_runtime_snapshot(opts) do
    Lock.with_lock(opts, fn ->
      with {:ok, runtime} <- State.read_runtime(opts),
           {:ok, installed_runtime} <- RuntimeWorkspace.read(opts),
           {:ok, secrets} <- State.read_secrets(opts) do
        {:ok, runtime, installed_runtime, secrets}
      end
    end)
  end

  defp compile_project do
    Mix.Task.reenable("compile")

    case Mix.Task.run("compile", ["--force"]) do
      _ -> :ok
    end
  rescue
    error -> {:error, {:compile_failed, error}}
  end

  defp ensure_no_in_flight_runs(runtime, secrets, opts) do
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    case secrets["service_token"] do
      token when is_binary(token) and token != "" ->
        case OrchestratorClient.in_flight_runs(base_url, token) do
          {:ok, []} -> :ok
          {:ok, run_ids} -> {:error, {:in_flight_runs, run_ids}}
          {:error, _reason} = error -> error
        end

      _ ->
        {:error, :missing_service_token}
    end
  end

  defp restart_runner(runtime, installed_runtime, secrets, opts) do
    runner_node = get_in(runtime, ["services", "runner", "node_name"])

    old_runner_pid = get_in(runtime, ["services", "runner", "pid"])

    if is_integer(old_runner_pid) and old_runner_pid > 0 do
      :ok = DevProcess.stop_pid(old_runner_pid)
    end

    node_names = %{runner_short: runner_sname(runner_node)}

    case DevProcess.start_service(
           RuntimeLaunch.runner_spec(installed_runtime, opts, node_names, secrets)
         ) do
      {:ok, info} ->
        updated_runtime =
          runtime
          |> put_in(["services", "runner", "pid"], info.pid)
          |> put_in(["services", "runner", "log_path"], info.log_path)

        case Lock.with_lock(opts, fn -> State.write_runtime(updated_runtime, opts) end) do
          :ok -> {:ok, updated_runtime}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:runner_restart_failed, reason}}
    end
  end

  @doc false
  @spec runner_sname(String.t()) :: String.t()
  def runner_sname(node_name) when is_binary(node_name) do
    node_name
    |> String.split("@", parts: 2)
    |> hd()
  end

  defp register_manifest_in_runner(version, runtime, secrets) do
    runner_node_name = get_in(runtime, ["services", "runner", "node_name"])

    RunnerControl.register_manifest(version,
      runner_node_name: runner_node_name,
      rpc_cookie: secrets["rpc_cookie"]
    )
  end

  defp write_manifest_cache(version, opts) do
    State.write_manifest_latest(
      %{
        "manifest_version_id" => version.manifest_version_id,
        "content_hash" => version.content_hash,
        "schema_version" => version.schema_version,
        "runner_contract_version" => version.runner_contract_version,
        "serialization_format" => version.serialization_format,
        "inserted_at" => datetime(version.inserted_at)
      },
      opts
    )
  end

  defp publish_manifest(version, runtime, secrets, opts) do
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    case secrets["service_token"] do
      token when is_binary(token) and token != "" ->
        OrchestratorClient.publish_manifest(base_url, token, %{
          manifest_version_id: version.manifest_version_id,
          manifest: version.manifest
        })

      _ ->
        {:error, :missing_service_token}
    end
  end

  defp activate_manifest(version, runtime, secrets, opts) do
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    case secrets["service_token"] do
      token when is_binary(token) and token != "" ->
        OrchestratorClient.activate_manifest(base_url, token, version.manifest_version_id)

      _ ->
        {:error, :missing_service_token}
    end
  end

  defp update_runtime_manifest(manifest_version_id, opts) do
    Lock.with_lock(opts, fn ->
      with {:ok, runtime} <- State.read_runtime(opts) do
        State.write_runtime(
          Map.put(runtime, "active_manifest_version_id", manifest_version_id),
          opts
        )
      end
    end)
  end

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
end

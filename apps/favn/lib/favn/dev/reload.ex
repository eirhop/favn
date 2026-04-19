defmodule Favn.Dev.Reload do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.Lock
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Paths
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RunnerControl
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @type reload_opt :: [root_dir: Path.t()]

  @spec run(reload_opt()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn ->
      with :ok <- ensure_running(opts),
           {:ok, runtime} <- State.read_runtime(opts),
           {:ok, secrets} <- State.read_secrets(opts),
           :ok <- ensure_no_in_flight_runs(runtime, secrets, opts),
           :ok <- compile_project(),
           {:ok, build} <- Favn.build_manifest(),
           {:ok, version} <- Favn.pin_manifest_version(build.manifest),
           {:ok, runtime} <- restart_runner(runtime, opts),
           :ok <- RunnerControl.register_manifest(version, opts),
           :ok <- write_manifest_cache(version, opts),
           {:ok, _published} <- publish_manifest(version, runtime, secrets, opts),
           {:ok, _activated} <- activate_manifest(version, runtime, secrets, opts),
           :ok <- update_runtime_manifest(runtime, version.manifest_version_id, opts) do
        IO.puts("Favn manifest reloaded: #{version.manifest_version_id}")
        :ok
      end
    end)
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      :stopped -> {:error, :stack_not_running}
      _ -> {:error, :stack_not_running}
    end
  end

  defp compile_project do
    Mix.Task.reenable("compile")

    case Mix.Task.run("compile", ["--force"]) do
      _ -> :ok
    end
  rescue
    error -> {:error, {:compile_failed, error}}
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

      _other ->
        {:error, :missing_service_token}
    end
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

      _other ->
        {:error, :missing_service_token}
    end
  end

  defp activate_manifest(version, runtime, secrets, opts) do
    base_url = runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url

    case secrets["service_token"] do
      token when is_binary(token) and token != "" ->
        OrchestratorClient.activate_manifest(base_url, token, version.manifest_version_id)

      _other ->
        {:error, :missing_service_token}
    end
  end

  defp update_runtime_manifest(runtime, manifest_version_id, opts) do
    runtime
    |> Map.put("active_manifest_version_id", manifest_version_id)
    |> then(&State.write_runtime(&1, opts))
  end

  defp restart_runner(runtime, opts) do
    root_dir = Paths.root_dir(opts)

    old_runner_pid = get_in(runtime, ["services", "runner", "pid"])

    if is_integer(old_runner_pid) and old_runner_pid > 0 do
      :ok = DevProcess.stop_pid(old_runner_pid)
    end

    case DevProcess.start_service(runner_spec(root_dir)) do
      {:ok, info} ->
        updated_runtime =
          put_in(runtime, ["services", "runner"], %{
            "pid" => info.pid,
            "log_path" => info.log_path
          })

        case State.write_runtime(updated_runtime, opts) do
          :ok -> {:ok, updated_runtime}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:runner_restart_failed, reason}}
    end
  end

  defp runner_spec(root_dir) do
    mix = System.find_executable("mix") || "mix"
    code = "Application.ensure_all_started(:favn_runner); Process.sleep(:infinity)"

    %{
      name: "runner",
      exec: mix,
      args: ["run", "--no-start", "--eval", code],
      cwd: Path.join(root_dir, "apps/favn_runner"),
      log_path: Paths.runner_log_path(root_dir),
      env: %{"MIX_ENV" => "dev"}
    }
  end

  defp datetime(nil), do: nil
  defp datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
end

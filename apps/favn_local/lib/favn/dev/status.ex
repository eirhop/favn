defmodule Favn.Dev.Status do
  @moduledoc """
  Local stack status inspection for project-scoped `.favn/` runtime state.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.State

  @type inspect_opts :: [root_dir: Path.t()]

  @doc """
  Returns current local stack status.
  """
  @spec inspect_stack(inspect_opts()) :: map()
  def inspect_stack(opts \\ []) when is_list(opts) do
    config = Config.resolve(opts)

    case State.read_runtime(opts) do
      {:ok, runtime} ->
        build_running_status(runtime, config, opts)

      {:error, :not_found} ->
        %{
          stack_status: :stopped,
          storage: config.storage,
          orchestrator_url: config.orchestrator_base_url,
          web_url: config.web_base_url,
          services: default_service_states(),
          active_manifest_version_id: nil,
          last_failure: normalize_last_failure(State.read_last_failure(opts))
        }

      {:error, reason} ->
        %{
          stack_status: :unknown,
          storage: config.storage,
          orchestrator_url: config.orchestrator_base_url,
          web_url: config.web_base_url,
          services: default_service_states(),
          active_manifest_version_id: nil,
          last_failure: normalize_last_failure(State.read_last_failure(opts)),
          error: reason
        }
    end
  end

  defp build_running_status(runtime, config, opts) do
    services =
      %{
        web: service_state(runtime, "web"),
        orchestrator: service_state(runtime, "orchestrator"),
        runner: service_state(runtime, "runner")
      }

    %{
      stack_status: summarize_stack_status(services),
      storage: runtime["storage"] || config.storage,
      orchestrator_url: runtime["orchestrator_base_url"] || config.orchestrator_base_url,
      web_url: runtime["web_base_url"] || config.web_base_url,
      services: services,
      active_manifest_version_id:
        runtime["active_manifest_version_id"] || manifest_from_cache(opts),
      last_failure: normalize_last_failure(State.read_last_failure(opts))
    }
  end

  defp manifest_from_cache(opts) do
    case State.read_manifest_latest(opts) do
      {:ok, manifest} -> manifest["manifest_version_id"]
      {:error, _reason} -> nil
    end
  end

  defp service_state(runtime, key) do
    case get_in(runtime, ["services", key]) do
      %{"pid" => pid} = service when is_integer(pid) and pid > 0 ->
        status = if process_alive?(pid), do: :running, else: :dead

        %{
          status: status,
          pid: pid,
          info: Map.drop(service, ["pid"])
        }

      _other ->
        %{status: :unknown, pid: nil, info: %{}}
    end
  end

  defp process_alive?(pid) when is_integer(pid) and pid > 0 do
    DevProcess.alive?(pid)
  end

  defp summarize_stack_status(services) do
    statuses = Map.values(services) |> Enum.map(& &1.status)

    cond do
      Enum.all?(statuses, &(&1 == :running)) -> :running
      Enum.any?(statuses, &(&1 == :running)) -> :partial
      Enum.all?(statuses, &(&1 in [:dead, :unknown])) -> :stale
      true -> :unknown
    end
  end

  defp normalize_last_failure({:ok, failure}), do: failure
  defp normalize_last_failure({:error, _reason}), do: nil

  defp default_service_states do
    %{
      web: %{status: :unknown, pid: nil, info: %{}},
      orchestrator: %{status: :unknown, pid: nil, info: %{}},
      runner: %{status: :unknown, pid: nil, info: %{}}
    }
  end
end

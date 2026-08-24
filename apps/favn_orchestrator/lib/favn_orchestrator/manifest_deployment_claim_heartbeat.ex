defmodule FavnOrchestrator.ManifestDeploymentClaimHeartbeat do
  @moduledoc false

  @enforce_keys [:pid]
  defstruct [:pid]

  @type t :: %__MODULE__{pid: pid()}

  @spec start((-> :ok | {:error, term()}), keyword()) :: t()
  def start(renew, opts) when is_function(renew, 0) and is_list(opts) do
    owner = self()
    interval_ms = Keyword.fetch!(opts, :interval_ms)

    pid =
      spawn(fn ->
        owner_monitor = Process.monitor(owner)
        loop(owner, owner_monitor, renew, interval_ms)
      end)

    %__MODULE__{pid: pid}
  end

  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = heartbeat) do
    Process.exit(heartbeat.pid, :kill)
    :ok
  end

  defp loop(owner, owner_monitor, renew, interval_ms) do
    receive do
      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    after
      interval_ms ->
        case renew_safely(renew) do
          :ok -> loop(owner, owner_monitor, renew, interval_ms)
          {:error, reason} -> Process.exit(owner, {:manifest_deployment_claim_lost, reason})
        end
    end
  end

  defp renew_safely(renew) do
    case renew.() do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_renewal_result, other}}
    end
  rescue
    error -> {:error, {:renewal_exception, error.__struct__}}
  catch
    kind, _reason -> {:error, {:renewal_exit, kind}}
  end
end

defmodule FavnOrchestrator.ManifestUploadHeartbeat do
  @moduledoc false

  @default_interval_ms 30_000

  @enforce_keys [:pid, :monitor, :ref]
  defstruct [:pid, :monitor, :ref]

  @type t :: %__MODULE__{pid: pid(), monitor: reference(), ref: reference()}

  @spec start((-> :ok | {:error, term()}), keyword()) :: t()
  def start(renew, opts \\ []) when is_function(renew, 0) do
    owner = self()
    ref = make_ref()
    interval_ms = Keyword.get(opts, :interval_ms, @default_interval_ms)

    {pid, monitor} =
      spawn_monitor(fn ->
        owner_monitor = Process.monitor(owner)
        loop(owner, owner_monitor, ref, renew, interval_ms)
      end)

    %__MODULE__{pid: pid, monitor: monitor, ref: ref}
  end

  @spec check(t(), timeout()) :: :ok | {:error, {:upload_lease_lost, term()}}
  def check(%__MODULE__{} = heartbeat, timeout \\ 0) do
    receive do
      {:manifest_upload_lease_lost, ref, reason} when ref == heartbeat.ref ->
        {:error, {:upload_lease_lost, reason}}

      {:DOWN, monitor, :process, pid, reason}
      when monitor == heartbeat.monitor and pid == heartbeat.pid ->
        {:error, {:upload_lease_lost, reason}}
    after
      timeout ->
        if Process.alive?(heartbeat.pid),
          do: :ok,
          else: {:error, {:upload_lease_lost, :heartbeat_stopped}}
    end
  end

  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = heartbeat) do
    Process.exit(heartbeat.pid, :kill)
    Process.demonitor(heartbeat.monitor, [:flush])
    flush_loss(heartbeat.ref)
    :ok
  end

  defp loop(owner, owner_monitor, ref, renew, interval_ms) do
    receive do
      {:stop, ^ref} ->
        Process.demonitor(owner_monitor, [:flush])
        :ok

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    after
      interval_ms ->
        case renew_safely(renew) do
          :ok -> loop(owner, owner_monitor, ref, renew, interval_ms)
          {:error, reason} -> send(owner, {:manifest_upload_lease_lost, ref, reason})
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

  defp flush_loss(ref) do
    receive do
      {:manifest_upload_lease_lost, ^ref, _reason} -> :ok
    after
      0 -> :ok
    end
  end
end

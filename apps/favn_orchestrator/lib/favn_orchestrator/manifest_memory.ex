defmodule FavnOrchestrator.ManifestMemory do
  @moduledoc """
  Fixed manifest-only bounds with automatic finite-cgroup admission.
  More RAM permits work but never increases archive, worker, result, or batch limits.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestMemory.Cgroup
  alias FavnOrchestrator.ManifestMemory.Slot
  alias FavnOrchestrator.ManifestMemory.Worker

  @mib 1_024 * 1_024
  @required_headroom 512 * @mib
  @package_heap 96 * @mib
  @package_result 64 * @mib
  @package_batch_result 64 * @mib
  @manifest_heap 256 * @mib
  @manifest_result 128 * @mib
  @index_result 128 * @mib
  @worker_timeout 30_000
  @phase_key {__MODULE__, :phase}

  @type capacity_error ::
          :manifest_capacity_busy | :manifest_capacity_unavailable | :memory_capacity_unknown

  @doc "Runs one upload or activation phase under exclusive local admission."
  @spec with_phase(atom(), (-> result), keyword()) :: result | {:error, capacity_error()}
        when result: term()
  def with_phase(_phase, fun, opts \\ []) when is_function(fun, 0) do
    slot = Keyword.get(opts, :slot, Slot)
    capacity_check = Keyword.get(opts, :capacity_check, &ensure_headroom/0)

    with {:ok, lease} <- Slot.acquire(server: slot) do
      previous_phase = Process.put(@phase_key, {slot, lease})

      try do
        with :ok <- capacity_check.(), do: fun.()
      after
        restore_phase(previous_phase)
        :ok = Slot.release(lease, server: slot)
      end
    end
  end

  @doc "Fails closed unless current finite-cgroup headroom is at least 512 MiB."
  @spec ensure_headroom(keyword()) :: :ok | {:error, capacity_error()}
  def ensure_headroom(opts \\ []) do
    case Cgroup.snapshot(opts) do
      {:ok, %{headroom_bytes: headroom}} when headroom >= @required_headroom -> :ok
      {:ok, _snapshot} -> {:error, :manifest_capacity_unavailable}
      {:error, _reason} -> {:error, :memory_capacity_unknown}
    end
  end

  @doc false
  def package_worker(fun, opts \\ []) do
    Worker.run(fun, @package_heap, @package_result,
      timeout: Keyword.get(opts, :timeout, @worker_timeout),
      measure: Keyword.get(opts, :measure, &Worker.retained_bytes/1),
      before_start: &track_phase_worker/1
    )
  end

  @doc false
  def manifest_worker(fun, opts \\ []) do
    Worker.run(fun, @manifest_heap, @manifest_result,
      timeout: Keyword.get(opts, :timeout, @worker_timeout),
      measure: Keyword.get(opts, :measure, &Worker.retained_bytes/1),
      before_start: &track_phase_worker/1
    )
  end

  @doc false
  def valid_package_batch?(packages) when is_list(packages),
    do: Worker.retained_bytes(packages) <= @package_batch_result

  @doc false
  def valid_version_size?(%Version{} = version),
    do: Worker.retained_bytes(version) <= @manifest_result

  @doc false
  def valid_index_size?(bytes), do: is_integer(bytes) and bytes <= @index_result

  @doc false
  def bounds do
    %{
      required_headroom: @required_headroom,
      package_heap: @package_heap,
      package_result: @package_result,
      package_batch_result: @package_batch_result,
      manifest_heap: @manifest_heap,
      manifest_result: @manifest_result,
      index_result: @index_result,
      worker_timeout: @worker_timeout
    }
  end

  defp track_phase_worker(worker) do
    case Process.get(@phase_key) do
      {slot, lease} -> Slot.track_worker(lease, worker, server: slot)
      nil -> :ok
    end
  end

  defp restore_phase(nil), do: Process.delete(@phase_key)
  defp restore_phase(previous), do: Process.put(@phase_key, previous)
end

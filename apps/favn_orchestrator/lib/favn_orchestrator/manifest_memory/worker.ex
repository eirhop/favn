defmodule FavnOrchestrator.ManifestMemory.Worker do
  @moduledoc """
  Runs one read-only manifest operation in a monitored, heap-bounded process.

  The caller receives control only after both the serialized result and the
  matching process termination have been observed.
  """

  @term_multiplier 4

  @type error ::
          :manifest_memory_budget_exceeded
          | :manifest_worker_timeout
          | :manifest_worker_failed

  @doc "Runs a callback under fixed heap, result, and timeout bounds."
  @spec run((-> result), pos_integer(), pos_integer(), keyword()) ::
          {:ok, result, non_neg_integer()} | {:error, error()}
        when result: term()
  def run(fun, heap_bytes, result_bytes, opts \\ [])
      when is_function(fun, 0) and is_integer(heap_bytes) and heap_bytes > 0 and
             is_integer(result_bytes) and result_bytes > 0 and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    measure = Keyword.get(opts, :measure, &retained_bytes/1)
    before_start = Keyword.get(opts, :before_start, fn _worker -> :ok end)
    owner = self()

    {pid, reference} =
      spawn_monitor(fn ->
        run_worker(owner, fun, measure, heap_bytes, result_bytes)
      end)

    case call_before_start(before_start, pid) do
      :ok ->
        send(pid, {__MODULE__, owner, :start})
        await(pid, reference, timeout)

      _error ->
        Process.exit(pid, :kill)
        await_down(reference, pid)
        {:error, :manifest_worker_failed}
    end
  end

  @doc "Returns the conservative retained size used by every manifest term bound."
  @spec retained_bytes(term()) :: non_neg_integer()
  def retained_bytes(term), do: @term_multiplier * :erlang.external_size(term)

  defp run_worker(owner, fun, measure, heap_bytes, result_bytes) do
    Process.flag(:max_heap_size, %{
      size: max(div(heap_bytes, :erlang.system_info(:wordsize)), 1),
      kill: true,
      error_logger: false,
      include_shared_binaries: true
    })

    worker = self()
    watcher = spawn_link(fn -> watch_owner(owner, worker) end)

    receive do
      {__MODULE__, ^watcher, :watching} -> :ok
    end

    receive do
      {__MODULE__, ^owner, :start} -> :ok
    end

    result = fun.()
    retained_bytes = measure.(result)

    if retained_bytes <= result_bytes do
      send(owner, {{__MODULE__, self()}, :erlang.term_to_binary({result, retained_bytes})})
    else
      exit(:manifest_result_too_large)
    end
  end

  defp watch_owner(owner, worker) do
    owner_monitor = Process.monitor(owner)
    worker_monitor = Process.monitor(worker)
    send(worker, {__MODULE__, self(), :watching})

    receive do
      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        Process.exit(worker, :kill)
        await_down(worker_monitor, worker)

      {:DOWN, ^worker_monitor, :process, ^worker, _reason} ->
        Process.demonitor(owner_monitor, [:flush])
        :ok
    end
  end

  defp call_before_start(callback, pid) do
    callback.(pid)
  rescue
    _error -> {:error, :callback_failed}
  catch
    _kind, _reason -> {:error, :callback_failed}
  end

  defp await(pid, reference, timeout) do
    tag = {__MODULE__, pid}

    receive do
      {^tag, encoded} ->
        await_down_then_decode(pid, reference, encoded)

      {:DOWN, ^reference, :process, ^pid, reason} ->
        await_result_after_down(tag, reason)
    after
      timeout ->
        Process.exit(pid, :kill)
        await_down(reference, pid)
        drain_result(tag)
        {:error, :manifest_worker_timeout}
    end
  end

  defp await_down_then_decode(pid, reference, encoded) do
    receive do
      {:DOWN, ^reference, :process, ^pid, :normal} -> decode(encoded)
      {:DOWN, ^reference, :process, ^pid, reason} -> worker_error(reason)
    end
  end

  defp await_result_after_down(tag, :normal) do
    receive do
      {^tag, encoded} -> decode(encoded)
    after
      0 -> {:error, :manifest_worker_failed}
    end
  end

  defp await_result_after_down(_tag, reason), do: worker_error(reason)

  # sobelow_skip ["Misc.BinToTerm"]
  defp decode(encoded) when is_binary(encoded) do
    case :erlang.binary_to_term(encoded, [:safe]) do
      {result, retained_bytes} when is_integer(retained_bytes) and retained_bytes >= 0 ->
        {:ok, result, retained_bytes}

      _invalid ->
        {:error, :manifest_worker_failed}
    end
  rescue
    _invalid -> {:error, :manifest_worker_failed}
  end

  defp worker_error(reason) when reason in [:killed, :manifest_result_too_large],
    do: {:error, :manifest_memory_budget_exceeded}

  defp worker_error(_reason), do: {:error, :manifest_worker_failed}

  defp await_down(reference, pid) do
    receive do
      {:DOWN, ^reference, :process, ^pid, _reason} -> :ok
    end
  end

  defp drain_result(tag) do
    receive do
      {^tag, _encoded} -> :ok
    after
      0 -> :ok
    end
  end
end

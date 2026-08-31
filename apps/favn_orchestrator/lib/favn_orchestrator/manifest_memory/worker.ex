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
  # sobelow_skip ["Misc.BinToTerm"]
  def run(fun, heap_bytes, result_bytes, opts \\ [])
      when is_function(fun, 0) and is_integer(heap_bytes) and heap_bytes > 0 and
             is_integer(result_bytes) and result_bytes > 0 and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    measure = Keyword.get(opts, :measure, &retained_bytes/1)
    owner = self()

    {pid, reference} =
      spawn_monitor(fn ->
        Process.flag(:max_heap_size, %{
          size: max(div(heap_bytes, :erlang.system_info(:wordsize)), 1),
          kill: true,
          error_logger: false,
          include_shared_binaries: true
        })

        result = fun.()
        retained_bytes = measure.(result)

        if retained_bytes <= result_bytes do
          send(owner, {{__MODULE__, self()}, :erlang.term_to_binary({result, retained_bytes})})
        else
          exit(:manifest_result_too_large)
        end
      end)

    await(pid, reference, timeout)
  end

  @doc "Returns the conservative retained size used by every manifest term bound."
  @spec retained_bytes(term()) :: non_neg_integer()
  def retained_bytes(term), do: @term_multiplier * :erlang.external_size(term)

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

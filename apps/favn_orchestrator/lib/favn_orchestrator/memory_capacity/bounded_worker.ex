defmodule FavnOrchestrator.MemoryCapacity.BoundedWorker do
  @moduledoc """
  Runs one read-only decode or validation stage in a monitored process.

  Exact byte limits and node admission are the primary protection. The process
  heap limit is a final backstop that also counts shared binaries.
  """

  @default_timeout 30_000

  alias FavnOrchestrator.MemoryCapacity.Budget

  @doc "Runs a read-only callback under a conservative process heap ceiling."
  @spec run((-> result), pos_integer(), keyword()) ::
          result | {:error, :manifest_memory_budget_exceeded | :worker_timeout | :worker_failed}
        when result: term()
  def run(fun, max_bytes, opts \\ [])
      when is_function(fun, 0) and is_integer(max_bytes) and max_bytes > 0 and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    owner = self()

    {pid, reference} =
      spawn_monitor(fn ->
        words = max(div(max_bytes, :erlang.system_info(:wordsize)), 1)

        Process.flag(:max_heap_size, %{
          size: words,
          kill: true,
          error_logger: false,
          include_shared_binaries: true
        })

        send(owner, {{__MODULE__, self()}, fun.()})
      end)

    tag = {__MODULE__, pid}

    receive do
      {^tag, result} ->
        receive do
          {:DOWN, ^reference, :process, ^pid, :normal} -> result
          {:DOWN, ^reference, :process, ^pid, _reason} -> {:error, :worker_failed}
        after
          1_000 ->
            Process.exit(pid, :kill)
            await_worker_down(reference, pid)
            result
        end

      {:DOWN, ^reference, :process, ^pid, :normal} ->
        receive do
          {^tag, result} -> result
        after
          0 -> {:error, :worker_failed}
        end

      {:DOWN, ^reference, :process, ^pid, :killed} ->
        {:error, :manifest_memory_budget_exceeded}

      {:DOWN, ^reference, :process, ^pid, _reason} ->
        {:error, :worker_failed}
    after
      timeout ->
        Process.exit(pid, :kill)

        drain_timed_out_worker(tag, reference, pid)

        {:error, :worker_timeout}
    end
  end

  @doc "Runs a callback and transfers its bounded result without copying its live heap."
  @spec run_serialized((-> result), pos_integer(), pos_integer(), keyword()) ::
          result | {:error, :manifest_memory_budget_exceeded | :worker_timeout | :worker_failed}
        when result: term()
  # sobelow_skip ["Misc.BinToTerm"]
  def run_serialized(fun, max_heap_bytes, max_result_bytes, opts \\ [])
      when is_function(fun, 0) and is_integer(max_heap_bytes) and max_heap_bytes > 0 and
             is_integer(max_result_bytes) and max_result_bytes > 0 and is_list(opts) do
    result =
      run(
        fn ->
          value = fun.()

          if Budget.retained_term_bytes(value) <= max_result_bytes do
            {:serialized_worker_result, :erlang.term_to_binary(value)}
          else
            {:error, :manifest_memory_budget_exceeded}
          end
        end,
        max_heap_bytes,
        opts
      )

    case result do
      {:serialized_worker_result, encoded} when is_binary(encoded) ->
        :erlang.binary_to_term(encoded, [:safe])

      other ->
        other
    end
  rescue
    _invalid_result -> {:error, :worker_failed}
  end

  defp drain_timed_out_worker(tag, reference, pid) do
    await_worker_down(reference, pid)

    receive do
      {^tag, _late_result} -> :ok
    after
      0 -> :ok
    end
  end

  defp await_worker_down(reference, pid) do
    receive do
      {:DOWN, ^reference, :process, ^pid, _reason} -> :ok
    end
  end
end

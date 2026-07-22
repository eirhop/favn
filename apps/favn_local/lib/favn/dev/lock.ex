defmodule Favn.Dev.Lock do
  @moduledoc """
  Crash-recoverable file lock used to serialize project-local mutations.

  Lock ownership includes the operating-system process identity and Linux boot
  identity. A later CLI process may reclaim the lock only after it proves that
  the recorded owner no longer exists. The in-VM owner also monitors the
  calling process so an abruptly terminated task releases promptly.
  """

  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @default_timeout_ms 5_000
  @poll_interval_ms 50

  @type lock_opt :: {:root_dir, Path.t()} | {:lock_timeout_ms, non_neg_integer()}

  @doc """
  Runs `fun` under an exclusive project-adjacent lock file.

  The lock sits beside `.favn/` so a confirmed reset cannot remove the lock
  while its destructive callback is still running. On Linux, a lock left by a
  killed CLI process is reclaimed after its PID, boot ID, and process start
  time prove that the exact owner is gone.
  """
  @spec with_lock([lock_opt()], (-> term())) :: term()
  def with_lock(opts \\ [], fun) when is_list(opts) and is_function(fun, 0) do
    with :ok <- State.ensure_layout(opts),
         {:ok, timeout_ms} <- lock_timeout(opts),
         {:ok, owner, monitor} <- start_owner(opts, timeout_ms) do
      try do
        fun.()
      after
        release_owner(owner, monitor)
      end
    end
  end

  defp lock_timeout(opts) do
    case Keyword.get(opts, :lock_timeout_ms, @default_timeout_ms) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms >= 0 -> {:ok, timeout_ms}
      _invalid -> {:error, {:lock_failed, :invalid_timeout}}
    end
  end

  defp start_owner(opts, timeout_ms) do
    caller = self()
    request = make_ref()

    {owner, monitor} =
      spawn_monitor(fn ->
        caller_monitor = Process.monitor(caller)

        case acquire_lock(opts, timeout_ms, caller_monitor) do
          {:ok, ownership} ->
            send(caller, {request, :acquired})
            await_release(caller, caller_monitor, request, ownership)

          {:error, _reason} = error ->
            send(caller, {request, error})
        end
      end)

    receive do
      {^request, :acquired} ->
        {:ok, {owner, request}, monitor}

      {^request, {:error, _reason} = error} ->
        Process.demonitor(monitor, [:flush])
        error

      {:DOWN, ^monitor, :process, ^owner, reason} ->
        {:error, {:lock_failed, {:owner_exited, reason}}}
    end
  end

  defp release_owner({owner, request}, monitor) do
    send(owner, {request, :release, self()})

    receive do
      {^request, :released} -> :ok
      {:DOWN, ^monitor, :process, ^owner, _reason} -> :ok
    after
      @default_timeout_ms -> :ok
    end

    Process.demonitor(monitor, [:flush])
  end

  defp await_release(caller, caller_monitor, request, ownership) do
    receive do
      {^request, :release, ^caller} ->
        release_lock(ownership)
        send(caller, {request, :released})

      {:DOWN, ^caller_monitor, :process, ^caller, _reason} ->
        release_lock(ownership)
    end
  end

  defp acquire_lock(opts, timeout_ms, caller_monitor) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    path = opts |> Paths.root_dir() |> Paths.lock_path()
    owner_id = owner_id()
    ownership = owner_metadata(path, owner_id)
    candidate = lock_candidate(path, owner_id)

    with :ok <- persist_candidate(candidate, ownership) do
      try do
        do_acquire_lock(path, candidate, owner_id, ownership, deadline, caller_monitor)
      after
        _ = File.rm(candidate)
      end
    end
  end

  defp do_acquire_lock(path, candidate, owner_id, ownership, deadline, caller_monitor) do
    receive do
      {:DOWN, ^caller_monitor, :process, _caller, _reason} ->
        exit(:normal)
    after
      0 ->
        case File.ln(candidate, path) do
          :ok ->
            {:ok, ownership}

          {:error, :eexist} ->
            retry_or_reclaim(
              path,
              candidate,
              owner_id,
              ownership,
              deadline,
              caller_monitor
            )

          {:error, reason} ->
            {:error, {:lock_failed, reason}}
        end
    end
  end

  defp persist_candidate(candidate, ownership) do
    encoded = JSON.encode!(ownership)

    case File.write(candidate, encoded <> "\n", [:exclusive, :sync]) do
      :ok ->
        case File.chmod(candidate, 0o600) do
          :ok -> :ok
          {:error, reason} -> candidate_error(candidate, reason)
        end

      {:error, reason} ->
        candidate_error(candidate, reason)
    end
  end

  defp candidate_error(candidate, reason) do
    _ = File.rm(candidate)
    {:error, {:lock_failed, {:owner_metadata, reason}}}
  end

  defp retry_or_reclaim(
         path,
         candidate,
         owner_id,
         ownership,
         deadline,
         caller_monitor
       ) do
    case owner_status(path) do
      :stale ->
        with :ok <- quarantine_stale_lock(path, owner_id) do
          do_acquire_lock(path, candidate, owner_id, ownership, deadline, caller_monitor)
        end

      :live ->
        retry_acquire(
          path,
          candidate,
          owner_id,
          ownership,
          deadline,
          caller_monitor
        )
    end
  end

  defp retry_acquire(path, candidate, owner_id, ownership, deadline, caller_monitor) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      {:error, {:lock_failed, :timeout}}
    else
      Process.sleep(min(@poll_interval_ms, deadline - now))
      do_acquire_lock(path, candidate, owner_id, ownership, deadline, caller_monitor)
    end
  end

  defp owner_status(path) do
    with {:ok, encoded} <- File.read(path),
         {:ok, owner} when is_map(owner) <- JSON.decode(encoded) do
      linux_owner_status(owner)
    else
      _unreadable_or_invalid -> :live
    end
  end

  defp linux_owner_status(%{
         "pid" => pid,
         "boot_id" => boot_id,
         "process_start_ticks" => start_ticks
       })
       when is_binary(pid) and is_binary(boot_id) and is_binary(start_ticks) do
    if Regex.match?(~r/\A[1-9][0-9]*\z/, pid) do
      case linux_boot_id() do
        {:ok, ^boot_id} -> linux_process_status(pid, start_ticks)
        {:ok, _different_boot} -> :stale
        {:error, _unavailable} -> :live
      end
    else
      :live
    end
  end

  defp linux_owner_status(_owner), do: :live

  defp linux_process_status(pid, expected_start_ticks) do
    case linux_process_start_ticks(pid) do
      {:ok, ^expected_start_ticks} -> :live
      {:ok, _reused_pid} -> :stale
      {:error, :enoent} -> :stale
      {:error, :enotdir} -> :stale
      {:error, _unavailable} -> :live
    end
  end

  defp quarantine_stale_lock(path, owner_id) do
    quarantine = "#{path}.stale.#{owner_id}"

    case File.rename(path, quarantine) do
      :ok ->
        case File.rm_rf(quarantine) do
          {:ok, _removed} -> :ok
          {:error, reason, _file} -> {:error, {:lock_failed, {:stale_cleanup, reason}}}
        end

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, {:lock_failed, {:stale_quarantine, reason}}}
    end
  end

  defp release_lock(%{"owner_id" => owner_id, "path" => path}) do
    with {:ok, encoded} <- File.read(path),
         {:ok, %{"owner_id" => ^owner_id}} <- JSON.decode(encoded) do
      _ = File.rm(path)
      :ok
    else
      _no_longer_owned -> :ok
    end
  end

  defp owner_metadata(path, owner_id) do
    %{
      "schema_version" => 1,
      "owner_id" => owner_id,
      "path" => path,
      "pid" => System.pid(),
      "boot_id" => linux_value(&linux_boot_id/0),
      "process_start_ticks" => linux_value(fn -> linux_process_start_ticks(System.pid()) end),
      "acquired_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp lock_candidate(path, owner_id) do
    path
    |> Path.dirname()
    |> Paths.favn_dir()
    |> Path.join(".lock-candidate-#{owner_id}")
  end

  defp linux_value(fun) do
    case fun.() do
      {:ok, value} -> value
      {:error, _reason} -> nil
    end
  end

  defp linux_boot_id do
    case File.read("/proc/sys/kernel/random/boot_id") do
      {:ok, boot_id} -> {:ok, String.trim(boot_id)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp linux_process_start_ticks(pid) when is_binary(pid) do
    with {:ok, stat} <- File.read("/proc/#{pid}/stat"),
         close when is_integer(close) <- last_close_parenthesis(stat),
         fields <- stat |> binary_part(close + 2, byte_size(stat) - close - 2) |> String.split(),
         start_ticks when is_binary(start_ticks) <- Enum.at(fields, 19) do
      {:ok, start_ticks}
    else
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :invalid_proc_stat}
    end
  end

  defp last_close_parenthesis(stat) do
    case :binary.matches(stat, ")") do
      [] -> nil
      matches -> matches |> List.last() |> elem(0)
    end
  end

  defp owner_id do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end

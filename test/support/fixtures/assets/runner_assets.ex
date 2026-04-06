defmodule Favn.Test.Fixtures.Assets.Runner.RunnerAssets do
  use Favn.Assets

  @asset true
  def base(ctx), do: {:ok, %{partition: ctx.params[:partition]}}

  @asset true
  @depends :base
  def transform(_ctx), do: :ok

  @asset true
  @depends :base
  def invalid_return(_ctx), do: {:ok, :bad_shape}

  @asset true
  @depends :transform
  def final(_ctx), do: :ok

  @asset true
  @depends :transform
  def target_only(_ctx), do: :ok

  @asset true
  @depends :base
  def crashes(_ctx), do: raise("boom")

  @asset true
  @depends :base
  def returns_error(_ctx), do: {:error, :domain_failure}

  @asset true
  @depends :base
  def returns_timeout_error(_ctx), do: {:error, :timeout}

  @asset true
  def transient_then_ok(ctx) do
    if ctx.attempt == 1 do
      Process.sleep(50)
      raise "transient"
    else
      :ok
    end
  end

  @asset true
  def exits_then_ok(ctx) do
    if ctx.attempt == 1 do
      Process.exit(self(), :transient_exit)
    else
      :ok
    end
  end

  @asset true
  @depends :returns_error
  def after_error(_ctx), do: :ok

  @asset true
  def slow_asset(_ctx) do
    Process.sleep(100)
    :ok
  end

  @asset true
  def announce_source(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announced_run_id, ctx.run_id})
    end

    Process.sleep(60)
    :ok
  end

  @asset true
  @depends :announce_source
  def announce_target(_ctx), do: :ok

  @asset true
  @depends :announce_source
  def announce_downstream_fail(_ctx), do: {:error, :announce_downstream_failed}

  @asset true
  @depends :announce_source
  def announce_branch_ok(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_branch_ok_run_id, ctx.run_id})
    end

    :ok
  end

  @asset true
  @depends :announce_source
  def announce_branch_fail(ctx) do
    Process.sleep(40)

    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_branch_fail_run_id, ctx.run_id})
    end

    {:error, :announce_branch_failed}
  end

  @asset true
  @depends :announce_branch_ok
  @depends :announce_branch_fail
  def announce_branch_join(_ctx), do: :ok

  @asset true
  def with_meta(_ctx), do: {:ok, %{row_count: 123, source: :test}}

  @asset true
  def parallel_root(_ctx), do: :ok

  @asset true
  @depends :parallel_root
  def parallel_a(ctx), do: tracked_success(ctx, :parallel_a, 80)

  @asset true
  @depends :parallel_root
  def parallel_b(ctx), do: tracked_success(ctx, :parallel_b, 80)

  @asset true
  @depends :parallel_root
  def parallel_c(ctx), do: tracked_success(ctx, :parallel_c, 80)

  @asset true
  @depends :parallel_a
  @depends :parallel_b
  @depends :parallel_c
  def parallel_join(_ctx), do: :ok

  @asset true
  @depends :parallel_root
  def parallel_fail(ctx) do
    tracked_start(ctx, :parallel_fail)
    Process.sleep(25)
    tracked_finish(ctx, :parallel_fail)
    {:error, :parallel_failure}
  end

  @asset true
  @depends :parallel_root
  def parallel_slow(ctx), do: tracked_success(ctx, :parallel_slow, 120)

  @asset true
  @depends :parallel_slow
  def parallel_after_slow(ctx), do: tracked_success(ctx, :parallel_after_slow, 20)

  @asset true
  @depends :parallel_fail
  @depends :parallel_after_slow
  def parallel_terminal(_ctx), do: :ok

  @asset true
  @depends :parallel_root
  def hard_crash(_ctx), do: Process.exit(self(), :kill)

  defp tracked_success(ctx, name, sleep_ms) do
    tracked_start(ctx, name)
    Process.sleep(sleep_ms)
    tracked_finish(ctx, name)
    :ok
  end

  defp tracked_start(ctx, name) do
    maybe_track_counter(ctx.params[:counter], 1)

    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:asset_started, name, System.monotonic_time(:millisecond)})
    end

    :ok
  end

  defp tracked_finish(ctx, name) do
    maybe_track_counter(ctx.params[:counter], -1)

    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:asset_finished, name, System.monotonic_time(:millisecond)})
    end

    :ok
  end

  defp maybe_track_counter(nil, _delta), do: :ok

  defp maybe_track_counter(counter, delta) do
    current = :atomics.add_get(counter, 1, delta)

    if delta > 0 do
      update_max(counter, current)
    end

    :ok
  end

  defp update_max(counter, current) do
    max_seen = :atomics.get(counter, 2)

    cond do
      current <= max_seen -> :ok
      :atomics.compare_exchange(counter, 2, max_seen, current) == :ok -> :ok
      true -> update_max(counter, current)
    end
  end
end

defmodule Favn.Test.Fixtures.Assets.Runner.TerminalFailingStore do
  @behaviour Favn.Storage.Adapter

  @counter_key {__MODULE__, :put_count}

  @impl true
  def child_spec(_opts), do: :none

  @impl true
  def put_run(_run, _opts) do
    count = :persistent_term.get(@counter_key, 0)
    :persistent_term.put(@counter_key, count + 1)

    if count == 7 do
      {:error, :terminal_write_failed}
    else
      :ok
    end
  end

  @impl true
  def get_run(_run_id, _opts), do: {:error, :not_found}

  @impl true
  def list_runs(_opts, _adapter_opts), do: {:ok, []}

  def reset!, do: :persistent_term.put(@counter_key, 0)
end

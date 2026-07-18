defmodule Favn.Test.Fixtures.Assets.Runner.RunnerAssets do
  @moduledoc false
  use Favn.MultiAsset

  asset :base do
  end

  asset :transform do
    depends(:base)
  end

  asset :invalid_return do
    depends(:base)
  end

  asset :final do
    depends(:transform)
  end

  asset :target_only do
    depends(:transform)
  end

  asset :crashes do
    depends(:base)
  end

  asset :returns_error do
    depends(:base)
  end

  asset :returns_timeout_error do
    depends(:base)
  end

  asset :transient_then_ok do
  end

  asset :exits_then_ok do
  end

  asset :after_error do
    depends(:returns_error)
  end

  asset :slow_asset do
  end

  asset :announce_source do
  end

  asset :announce_target do
    depends(:announce_source)
  end

  asset :announce_downstream_fail do
    depends(:announce_source)
  end

  asset :announce_branch_ok do
    depends(:announce_source)
  end

  asset :announce_branch_fail do
    depends(:announce_source)
  end

  asset :announce_branch_join do
    depends(:announce_branch_ok)
    depends(:announce_branch_fail)
  end

  asset :announce_chain_a do
  end

  asset :announce_chain_b do
    depends(:announce_chain_a)
  end

  asset :announce_chain_c do
    depends(:announce_chain_b)
  end

  asset :with_meta do
  end

  asset :parallel_root do
  end

  asset :parallel_a do
    depends(:parallel_root)
  end

  asset :parallel_b do
    depends(:parallel_root)
  end

  asset :parallel_c do
    depends(:parallel_root)
  end

  asset :parallel_join do
    depends(:parallel_a)
    depends(:parallel_b)
    depends(:parallel_c)
  end

  asset :parallel_fail do
    depends(:parallel_root)
  end

  asset :parallel_slow do
    depends(:parallel_root)
  end

  asset :parallel_after_slow do
    depends(:parallel_slow)
  end

  asset :parallel_terminal do
    depends(:parallel_fail)
    depends(:parallel_after_slow)
  end

  asset :hard_crash do
    depends(:parallel_root)
  end

  def base(ctx), do: {:ok, %{partition: ctx.params[:partition]}}

  def transform(_ctx), do: :ok

  def invalid_return(_ctx), do: {:ok, :bad_shape}

  def final(_ctx), do: :ok

  def target_only(_ctx), do: :ok

  def crashes(_ctx), do: raise("boom")

  def returns_error(_ctx), do: {:error, :domain_failure}

  def returns_timeout_error(_ctx), do: {:error, :timeout}

  def transient_then_ok(ctx) do
    if ctx.attempt == 1 do
      Process.sleep(50)
      raise "transient"
    else
      :ok
    end
  end

  def exits_then_ok(ctx) do
    if ctx.attempt == 1 do
      Process.exit(self(), :transient_exit)
    else
      :ok
    end
  end

  def after_error(_ctx), do: :ok

  def slow_asset(_ctx) do
    Process.sleep(100)
    :ok
  end

  def announce_source(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announced_run_id, ctx.run_id})
    end

    Process.sleep(60)
    :ok
  end

  def announce_target(_ctx), do: :ok

  def announce_downstream_fail(_ctx), do: {:error, :announce_downstream_failed}

  def announce_branch_ok(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_branch_ok_run_id, ctx.run_id})
    end

    :ok
  end

  def announce_branch_fail(ctx) do
    Process.sleep(40)

    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_branch_fail_run_id, ctx.run_id})
    end

    {:error, :announce_branch_failed}
  end

  def announce_branch_join(_ctx), do: :ok

  def announce_chain_a(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_chain_a_run_id, ctx.run_id})
    end

    :ok
  end

  def announce_chain_b(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_chain_b_run_id, ctx.run_id})
    end

    :ok
  end

  def announce_chain_c(ctx) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announce_chain_c_run_id, ctx.run_id})
    end

    {:error, :announce_chain_failed}
  end

  def with_meta(_ctx), do: {:ok, %{row_count: 123, source: :test}}

  def parallel_root(_ctx), do: :ok

  def parallel_a(ctx), do: tracked_success(ctx, :parallel_a, 80)

  def parallel_b(ctx), do: tracked_success(ctx, :parallel_b, 80)

  def parallel_c(ctx), do: tracked_success(ctx, :parallel_c, 80)

  def parallel_join(_ctx), do: :ok

  def parallel_fail(ctx) do
    tracked_start(ctx, :parallel_fail)
    Process.sleep(25)
    tracked_finish(ctx, :parallel_fail)
    {:error, :parallel_failure}
  end

  def parallel_slow(ctx), do: tracked_success(ctx, :parallel_slow, 120)

  def parallel_after_slow(ctx), do: tracked_success(ctx, :parallel_after_slow, 20)

  def parallel_terminal(_ctx), do: :ok

  def hard_crash(_ctx), do: Process.exit(self(), :kill)

  @doc false
  def asset(ctx) do
    {_module, name} = ctx.asset.ref
    apply(__MODULE__, name, [ctx])
  end

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

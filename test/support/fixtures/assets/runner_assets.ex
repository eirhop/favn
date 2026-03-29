defmodule Favn.Test.Fixtures.Assets.Runner.RunnerAssets do
  use Favn.Assets

  @asset true
  def base(ctx, _deps) do
    {:ok, %Favn.Asset.Output{output: {:base, ctx.params[:partition]}}}
  end

  @asset depends_on: [:base]
  def transform(_ctx, deps) do
    {:ok, %Favn.Asset.Output{output: {:transform, Map.fetch!(deps, {__MODULE__, :base})}}}
  end

  @asset depends_on: [:base]
  def invalid_return(_ctx, _deps) do
    {:ok, :bad_shape}
  end

  @asset depends_on: [:transform]
  def final(_ctx, deps) do
    {:ok, %Favn.Asset.Output{output: {:final, Map.fetch!(deps, {__MODULE__, :transform})}}}
  end

  @asset depends_on: [:transform]
  def target_only(_ctx, deps) do
    {:ok, %Favn.Asset.Output{output: map_size(deps)}}
  end

  @asset depends_on: [:base]
  def crashes(_ctx, _deps) do
    raise "boom"
  end

  @asset depends_on: [:base]
  def returns_error(_ctx, _deps) do
    {:error, :domain_failure}
  end

  @asset depends_on: [:returns_error]
  def after_error(_ctx, _deps) do
    {:ok, %Favn.Asset.Output{output: :should_not_run}}
  end

  @asset true
  def slow_asset(_ctx, _deps) do
    Process.sleep(100)
    {:ok, %Favn.Asset.Output{output: :slow_ok}}
  end

  @asset true
  def announce_source(ctx, _deps) do
    if is_pid(ctx.params[:notify_pid]) do
      send(ctx.params[:notify_pid], {:announced_run_id, ctx.run_id})
    end

    Process.sleep(60)
    {:ok, %Favn.Asset.Output{output: :source_ok}}
  end

  @asset depends_on: [:announce_source]
  def announce_target(_ctx, deps) do
    {:ok, %Favn.Asset.Output{output: Map.fetch!(deps, {__MODULE__, :announce_source})}}
  end

  @asset true
  def with_meta(_ctx, _deps) do
    {:ok,
     %Favn.Asset.Output{
       output: {:rows, [1, 2, 3]},
       meta: %{row_count: 123, source: :test}
     }}
  end

  @asset true
  def parallel_root(_ctx, _deps), do: {:ok, %Favn.Asset.Output{output: :root}}

  @asset depends_on: [:parallel_root]
  def parallel_a(ctx, _deps), do: tracked_success(ctx, :parallel_a, 80)

  @asset depends_on: [:parallel_root]
  def parallel_b(ctx, _deps), do: tracked_success(ctx, :parallel_b, 80)

  @asset depends_on: [:parallel_root]
  def parallel_c(ctx, _deps), do: tracked_success(ctx, :parallel_c, 80)

  @asset depends_on: [:parallel_a, :parallel_b, :parallel_c]
  def parallel_join(_ctx, deps) do
    values =
      [:parallel_a, :parallel_b, :parallel_c]
      |> Enum.map(&Map.fetch!(deps, {__MODULE__, &1}))
      |> Enum.sort()

    {:ok, %Favn.Asset.Output{output: values}}
  end

  @asset depends_on: [:parallel_root]
  def parallel_fail(ctx, _deps) do
    tracked_start(ctx, :parallel_fail)
    Process.sleep(25)
    tracked_finish(ctx, :parallel_fail)
    {:error, :parallel_failure}
  end

  @asset depends_on: [:parallel_root]
  def parallel_slow(ctx, _deps), do: tracked_success(ctx, :parallel_slow, 120)

  @asset depends_on: [:parallel_slow]
  def parallel_after_slow(ctx, _deps), do: tracked_success(ctx, :parallel_after_slow, 20)

  @asset depends_on: [:parallel_fail, :parallel_after_slow]
  def parallel_terminal(_ctx, _deps), do: {:ok, %Favn.Asset.Output{output: :never}}

  @asset depends_on: [:parallel_root]
  def hard_crash(_ctx, _deps) do
    Process.exit(self(), :kill)
  end

  defp tracked_success(ctx, name, sleep_ms) do
    tracked_start(ctx, name)
    Process.sleep(sleep_ms)
    tracked_finish(ctx, name)
    {:ok, %Favn.Asset.Output{output: name}}
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
      current <= max_seen ->
        :ok

      :atomics.compare_exchange(counter, 2, max_seen, current) == :ok ->
        :ok

      true ->
        update_max(counter, current)
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

    # Deterministic failure on the terminal checkpoint for the standard
    # successful `:final` run flow after startup + per-step checkpoints.
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

  def reset!, do: :persistent_term.erase(@counter_key)
end

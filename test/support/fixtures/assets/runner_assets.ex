defmodule Flux.Test.Fixtures.Assets.Runner.RunnerAssets do
  use Flux.Assets

  @asset true
  def base(ctx, _deps) do
    {:ok, %Flux.Asset.Output{output: {:base, ctx.params[:partition]}}}
  end

  @asset depends_on: [:base]
  def transform(_ctx, deps) do
    {:ok, %Flux.Asset.Output{output: {:transform, Map.fetch!(deps, {__MODULE__, :base})}}}
  end

  @asset depends_on: [:base]
  def invalid_return(_ctx, _deps) do
    {:ok, :bad_shape}
  end

  @asset depends_on: [:transform]
  def final(_ctx, deps) do
    {:ok, %Flux.Asset.Output{output: {:final, Map.fetch!(deps, {__MODULE__, :transform})}}}
  end

  @asset depends_on: [:transform]
  def target_only(_ctx, deps) do
    {:ok, %Flux.Asset.Output{output: map_size(deps)}}
  end

  @asset depends_on: [:base]
  def crashes(_ctx, _deps) do
    raise "boom"
  end

  @asset true
  def with_meta(_ctx, _deps) do
    {:ok,
     %Flux.Asset.Output{
       output: {:rows, [1, 2, 3]},
       meta: %{row_count: 123, source: :test}
     }}
  end
end

defmodule Flux.Test.Fixtures.Assets.Runner.TerminalFailingStore do
  @behaviour Flux.Storage.Adapter

  @counter_key {__MODULE__, :put_count}

  @impl true
  def child_spec(_opts), do: :none

  @impl true
  def put_run(_run, _opts) do
    count = :persistent_term.get(@counter_key, 0)
    :persistent_term.put(@counter_key, count + 1)

    if count == 0 do
      :ok
    else
      {:error, :terminal_write_failed}
    end
  end

  @impl true
  def get_run(_run_id, _opts), do: {:error, :not_found}

  @impl true
  def list_runs(_opts, _adapter_opts), do: {:ok, []}

  def reset!, do: :persistent_term.erase(@counter_key)
end

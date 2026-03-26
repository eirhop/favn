defmodule Flux.Runner do
  @moduledoc """
  In-memory deterministic execution runner.

  The first implementation executes plan stages serially and fails fast on the
  first asset error.
  """

  alias Flux.Run
  alias Flux.Asset.Output
  alias Flux.Run.AssetResult
  alias Flux.Run.Context

  @typedoc """
  Options supported by the in-memory runner.
  """
  @type run_opts :: [
          dependencies: Flux.dependencies_mode(),
          params: map()
        ]

  @spec run(Flux.asset_ref(), run_opts()) :: {:ok, Run.t()} | {:error, Run.t() | term()}
  def run(target_ref, opts \\ []) when is_list(opts) do
    dependencies = Keyword.get(opts, :dependencies, :all)
    params = Keyword.get(opts, :params, %{})

    with :ok <- validate_params(params),
         {:ok, plan} <- Flux.plan_run(target_ref, dependencies: dependencies) do
      run = %Run{
        id: new_run_id(),
        target_refs: plan.target_refs,
        plan: plan,
        started_at: DateTime.utc_now(),
        params: params
      }

      with :ok <- Flux.Storage.put_run(run) do
        run = emit_run_event(run, :run_started, %{target_refs: run.target_refs})
        result = execute_plan(run)
        _ = persist_terminal_result(result)
        result
      end
    end
  end

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_params), do: {:error, :invalid_run_params}

  defp persist_terminal_result({:ok, %Run{} = run}) do
    Flux.Storage.put_run(run)
  end

  defp persist_terminal_result({:error, %Run{} = run}) do
    Flux.Storage.put_run(run)
  end

  defp persist_terminal_result(_result), do: :ok

  defp execute_plan(%Run{} = run) do
    case run.plan.stages |> Enum.with_index() |> Enum.reduce_while(run, &run_stage/2) do
      %Run{status: :error} = failed ->
        failed = emit_run_event(failed, :run_failed, %{error: failed.error})
        {:error, failed}

      %Run{} = finished ->
        finished = %Run{
          finished
          | status: :ok,
            finished_at: DateTime.utc_now(),
            target_outputs: Map.take(finished.outputs, finished.target_refs)
        }

        finished =
          emit_run_event(finished, :run_finished, %{target_outputs: finished.target_outputs})

        {:ok, finished}
    end
  end

  defp run_stage({refs, stage}, %Run{} = run) do
    refs
    |> Enum.reduce_while(run, fn ref, acc_run ->
      case execute_asset(acc_run, ref, stage) do
        {:ok, next_run} -> {:cont, next_run}
        {:error, failed_run} -> {:halt, failed_run}
      end
    end)
    |> then(fn
      %Run{status: :error} = failed -> {:halt, failed}
      %Run{} = next_run -> {:cont, next_run}
    end)
  end

  defp execute_asset(%Run{} = run, ref, stage) do
    started_at = DateTime.utc_now()
    started_monotonic = System.monotonic_time(:millisecond)
    node = Map.fetch!(run.plan.nodes, ref)
    run = emit_run_event(run, :asset_started, %{}, ref: ref, stage: stage)

    with {:ok, asset} <- Flux.Registry.get_asset(ref),
         {:ok, deps} <- dependency_outputs(run, node.upstream),
         ctx <- build_context(run, ref, stage),
         {:ok, %Output{} = asset_output} <- invoke_asset(asset, ctx, deps) do
      finished_at = DateTime.utc_now()

      result = %AssetResult{
        ref: ref,
        stage: stage,
        status: :ok,
        started_at: started_at,
        finished_at: finished_at,
        duration_ms: System.monotonic_time(:millisecond) - started_monotonic,
        output: asset_output.output,
        meta: asset_output.meta
      }

      run = %Run{
        run
        | outputs: Map.put(run.outputs, ref, asset_output.output),
          asset_results: Map.put(run.asset_results, ref, result)
      }

      run =
        emit_run_event(run, :asset_finished, %{duration_ms: result.duration_ms},
          ref: ref,
          stage: stage
        )

      {:ok, run}
    else
      {:error, reason} ->
        failed_run =
          fail_run(run, ref, stage, started_at, started_monotonic, normalize_reason(reason))

        failed_run =
          emit_run_event(failed_run, :asset_failed, %{error: failed_run.error},
            ref: ref,
            stage: stage
          )

        {:error, failed_run}
    end
  end

  defp dependency_outputs(%Run{} = run, depends_on) do
    Enum.reduce_while(depends_on, {:ok, %{}}, fn dep_ref, {:ok, acc} ->
      case Map.fetch(run.outputs, dep_ref) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, dep_ref, value)}}
        :error -> {:halt, {:error, {:missing_dependency_output, dep_ref}}}
      end
    end)
  end

  defp build_context(%Run{} = run, ref, stage) do
    %Context{
      run_id: run.id,
      target_refs: run.target_refs,
      current_ref: ref,
      params: run.params,
      run_started_at: run.started_at,
      stage: stage
    }
  end

  defp invoke_asset(asset, %Context{} = ctx, deps) do
    try do
      case apply(asset.module, asset.name, [ctx, deps]) do
        {:ok, %Output{} = asset_output} ->
          {:ok, asset_output}

        {:error, reason} ->
          {:error, reason}

        other ->
          {:error,
           {:invalid_return_shape, other,
            expected: "{:ok, %Flux.Asset.Output{}} | {:error, reason}"}}
      end
    rescue
      error ->
        {:error,
         %{
           kind: :error,
           reason: error,
           stacktrace: __STACKTRACE__,
           message: Exception.message(error)
         }}
    catch
      :throw, reason -> {:error, %{kind: :throw, reason: reason, stacktrace: __STACKTRACE__}}
      :exit, reason -> {:error, %{kind: :exit, reason: reason, stacktrace: __STACKTRACE__}}
    end
  end

  defp fail_run(run, ref, stage, started_at, started_monotonic, reason) do
    finished_at = DateTime.utc_now()

    result = %AssetResult{
      ref: ref,
      stage: stage,
      status: :error,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: System.monotonic_time(:millisecond) - started_monotonic,
      error: normalize_error(reason)
    }

    %Run{
      run
      | status: :error,
        finished_at: finished_at,
        error: %{ref: ref, stage: stage, reason: reason},
        asset_results: Map.put(run.asset_results, ref, result),
        target_outputs: Map.take(run.outputs, run.target_refs)
    }
  end

  defp normalize_reason(%{kind: _kind, reason: _reason, stacktrace: _stacktrace} = reason),
    do: reason

  defp normalize_reason(reason), do: reason

  defp normalize_error(%{kind: _kind, reason: _reason, stacktrace: _stacktrace} = error),
    do: error

  defp normalize_error(reason) do
    %{
      kind: :error,
      reason: reason,
      stacktrace: []
    }
  end

  defp new_run_id do
    binary = :crypto.strong_rand_bytes(16)
    <<a::32, b::16, c::16, d::16, e::48>> = binary

    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    Enum.join(
      [
        a |> Integer.to_string(16) |> String.pad_leading(8, "0"),
        b |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        c |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        d |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        e |> Integer.to_string(16) |> String.pad_leading(12, "0")
      ],
      "-"
    )
  end

  # Event publishing is best-effort observability only: run execution and run
  # storage persistence must continue even when PubSub delivery fails.
  defp emit_run_event(%Run{} = run, event, payload, opts \\ [])
       when is_atom(event) and is_map(payload) and is_list(opts) do
    next_seq = run.event_seq + 1

    _ =
      Flux.Events.publish_run_event(run.id, event, %{
        seq: next_seq,
        ref: Keyword.get(opts, :ref),
        stage: Keyword.get(opts, :stage),
        payload: payload
      })

    %Run{run | event_seq: next_seq}
  end
end

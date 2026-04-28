defmodule FavnRunner.Worker do
  @moduledoc false

  use GenServer

  alias Favn.Contracts.RunnerEvent
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Run.Context
  alias Favn.RuntimeConfig.Redactor, as: RuntimeConfigRedactor
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias FavnRunner.ContextBuilder
  alias FavnRunner.EventSink

  @type init_arg :: %{
          required(:server) => pid(),
          required(:execution_id) => String.t(),
          required(:work) => RunnerWork.t(),
          required(:version) => Version.t(),
          required(:asset) => Asset.t()
        }

  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{server: server, execution_id: execution_id} = state) do
    result = execute(state)
    send(server, {:runner_result, execution_id, result})
    {:stop, :normal, state}
  end

  defp execute(%{
         server: server,
         execution_id: execution_id,
         work: %RunnerWork{} = work,
         version: %Version{} = version,
         asset: %Asset{} = asset
       }) do
    started_at = DateTime.utc_now()

    emit_event(server, execution_id, work, :asset_started, %{asset_ref: asset.ref})

    result =
      case asset.type do
        :source ->
          execute_source_asset(asset)

        :elixir ->
          with {:ok, context} <- ContextBuilder.build(work, asset, execution_id) do
            asset
            |> execute_elixir_asset(context)
            |> redact_execution_result(asset, context)
          end

        :sql ->
          execute_sql_asset(asset, version, work)

        _ ->
          {:error, %{kind: :error, reason: {:unsupported_asset_type, asset.type}, stacktrace: []}}
      end

    finished_at = DateTime.utc_now()

    case result do
      {:ok, meta} ->
        emit_event(server, execution_id, work, :asset_succeeded, %{asset_ref: asset.ref})

        build_runner_result(
          work,
          version,
          [asset_result(asset, started_at, finished_at, :ok, meta, nil)],
          status: :ok
        )

      {:error, error} ->
        emit_event(server, execution_id, work, :asset_failed, %{
          asset_ref: asset.ref,
          error: error
        })

        build_runner_result(
          work,
          version,
          [asset_result(asset, started_at, finished_at, :error, %{}, error)],
          status: :error,
          error: error
        )
    end
  end

  defp execute_source_asset(%Asset{} = asset),
    do: {:ok, %{observed: true, relation: asset.relation}}

  defp execute_elixir_asset(%Asset{} = asset, %Context{} = context) do
    entrypoint = asset.execution[:entrypoint] || asset.name || :asset
    arity = asset.execution[:arity] || 1

    cond do
      not is_atom(asset.module) ->
        {:error, %{kind: :error, reason: {:invalid_module, asset.module}, stacktrace: []}}

      not is_atom(entrypoint) ->
        {:error, %{kind: :error, reason: {:invalid_entrypoint, entrypoint}, stacktrace: []}}

      arity != 1 ->
        {:error,
         %{
           kind: :error,
           reason: {:unsupported_entrypoint_arity, arity, expected: 1},
           stacktrace: []
         }}

      true ->
        invoke_asset(asset.module, entrypoint, context)
    end
  end

  defp redact_execution_result({:ok, meta}, %Asset{} = asset, %Context{} = context) do
    {:ok, RuntimeConfigRedactor.redact(meta, asset.runtime_config, context.config)}
  end

  defp redact_execution_result({:error, error}, %Asset{} = asset, %Context{} = context) do
    {:error, RuntimeConfigRedactor.redact(error, asset.runtime_config, context.config)}
  end

  defp invoke_asset(module, entrypoint, %Context{} = context) do
    case apply(module, entrypoint, [context]) do
      :ok ->
        {:ok, %{}}

      {:ok, meta} when is_map(meta) ->
        {:ok, meta}

      {:error, reason} ->
        {:error, %{kind: :error, reason: reason, stacktrace: []}}

      other ->
        {:error,
         %{
           kind: :error,
           reason:
             {:invalid_return_shape, other, expected: ":ok | {:ok, map()} | {:error, reason}"},
           stacktrace: []
         }}
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

  defp build_runner_result(%RunnerWork{} = work, %Version{} = version, asset_results, opts)
       when is_list(asset_results) and is_list(opts) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      status: Keyword.get(opts, :status, :ok),
      asset_results: asset_results,
      error: Keyword.get(opts, :error),
      metadata: Map.put(work.metadata, :execution_id, Keyword.get(opts, :execution_id))
    }
  end

  defp asset_result(%Asset{} = asset, started_at, finished_at, status, meta, error) do
    meta = RuntimeConfigRedactor.redact(meta, asset.runtime_config || %{})

    %AssetResult{
      ref: asset.ref,
      stage: 0,
      status: status,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: max(DateTime.diff(finished_at, started_at, :millisecond), 0),
      meta: meta,
      error: error,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [
        %{
          attempt: 1,
          started_at: started_at,
          finished_at: finished_at,
          duration_ms: max(DateTime.diff(finished_at, started_at, :millisecond), 0),
          status: status,
          meta: meta,
          error: error
        }
      ]
    }
  end

  defp emit_event(server, execution_id, work, event_type, payload) do
    event = %RunnerEvent{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      event_type: event_type,
      occurred_at: DateTime.utc_now(),
      payload: payload
    }

    EventSink.emit(server, execution_id, event)
  end

  defp execute_sql_asset(%Asset{} = asset, %Version{} = version, %RunnerWork{} = work) do
    SQLAssetRuntime.run_manifest(asset, version, work)
  rescue
    error ->
      %{
        kind: :error,
        reason: error,
        stacktrace: __STACKTRACE__,
        message: Exception.message(error)
      }
      |> then(&{:error, &1})
  end
end

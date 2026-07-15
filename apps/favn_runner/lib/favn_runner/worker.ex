defmodule FavnRunner.Worker do
  @moduledoc false

  use GenServer

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerEvent
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.Run.Context
  alias Favn.SQL.Client, as: SQLClient
  alias Favn.RuntimeConfig.Redactor, as: RuntimeConfigRedactor
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias FavnRunner.ContextBuilder
  alias FavnRunner.EventSink
  alias FavnRunner.LogSink
  alias FavnRunner.RuntimeConfigDiagnostic

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
    emit_log(server, execution_id, work, asset, 1, :info, "asset execution started")

    result =
      case asset.type do
        :source ->
          execute_source_asset(asset)

        :elixir ->
          case ContextBuilder.build(work, asset, execution_id) do
            {:ok, context} ->
              asset
              |> execute_elixir_asset(context)
              |> redact_execution_result(asset, context)

            {:error, error} ->
              {:error,
               RunnerError.normalize(
                 RuntimeConfigDiagnostic.asset_resolution_failed(error, asset),
                 retryable?: false
               )}
          end

        :sql ->
          execute_sql_asset(asset, version, work)

        _ ->
          {:error,
           RunnerError.normalize({:unsupported_asset_type, asset.type},
             type: :unsupported_asset_type,
             retryable?: false
           )}
      end

    finished_at = DateTime.utc_now()

    case result do
      {:ok, meta} ->
        emit_event(server, execution_id, work, :asset_succeeded, %{asset_ref: asset.ref})
        emit_log(server, execution_id, work, asset, 2, :info, "asset execution finished")

        build_runner_result(
          work,
          version,
          [asset_result(work, asset, started_at, finished_at, :ok, meta, nil)],
          status: :ok
        )

      {:error, error} ->
        emit_event(server, execution_id, work, :asset_failed, %{
          asset_ref: asset.ref,
          error: error
        })

        emit_log(server, execution_id, work, asset, 2, :error, "asset execution failed", %{
          error: error
        })

        build_runner_result(
          work,
          version,
          [asset_result(work, asset, started_at, finished_at, :error, %{}, error)],
          status: :error,
          error: error
        )

      {:error, error, meta} when is_map(meta) ->
        emit_event(server, execution_id, work, :asset_failed, %{
          asset_ref: asset.ref,
          error: error
        })

        emit_log(server, execution_id, work, asset, 2, :error, "asset execution failed", %{
          error: error,
          quality_status: Map.get(meta, :quality_status)
        })

        build_runner_result(
          work,
          version,
          [asset_result(work, asset, started_at, finished_at, :error, meta, error)],
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
        {:error,
         RunnerError.normalize({:invalid_module, asset.module},
           type: :invalid_module,
           retryable?: false
         )}

      not is_atom(entrypoint) ->
        {:error,
         RunnerError.normalize({:invalid_entrypoint, entrypoint},
           type: :invalid_entrypoint,
           retryable?: false
         )}

      arity != 1 ->
        {:error,
         RunnerError.normalize({:unsupported_entrypoint_arity, arity, expected: 1},
           type: :unsupported_entrypoint_arity,
           retryable?: false
         )}

      true ->
        with_asset_sql_scope(asset, fn -> invoke_asset(asset.module, entrypoint, context) end)
    end
  end

  defp with_asset_sql_scope(%Asset{relation: relation}, fun) when is_function(fun, 0) do
    case sql_scope(relation) do
      {connection, catalogs} ->
        SQLClient.with_default_required_catalogs(connection, catalogs, fun)

      nil ->
        fun.()
    end
  end

  defp sql_scope(%RelationRef{connection: connection, catalog: catalog})
       when is_atom(connection) do
    case catalog_name(catalog) do
      nil -> nil
      catalog -> {connection, [catalog]}
    end
  end

  defp sql_scope(_relation), do: nil

  defp catalog_name(catalog) when is_binary(catalog) and catalog != "", do: catalog

  defp catalog_name(catalog) when is_atom(catalog) and not is_nil(catalog),
    do: Atom.to_string(catalog)

  defp catalog_name(_catalog), do: nil

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
        {:error, RunnerError.normalize(reason)}

      other ->
        {:error,
         RunnerError.normalize(
           {:invalid_return_shape, other, expected: ":ok | {:ok, map()} | {:error, reason}"},
           type: :invalid_return_shape,
           retryable?: false
         )}
    end
  rescue
    error ->
      {:error, RunnerError.exception(:error, error, __STACKTRACE__)}
  catch
    :throw, reason -> {:error, RunnerError.exception(:throw, reason, __STACKTRACE__)}
    :exit, reason -> {:error, RunnerError.exception(:exit, reason, __STACKTRACE__)}
  end

  defp build_runner_result(%RunnerWork{} = work, %Version{} = version, asset_results, opts)
       when is_list(asset_results) and is_list(opts) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      status: Keyword.get(opts, :status, :ok),
      asset_results: asset_results,
      error: normalize_error(Keyword.get(opts, :error)),
      metadata:
        Map.put(
          RunnerWork.lifecycle_metadata(work),
          :execution_id,
          Keyword.get(opts, :execution_id)
        )
    }
  end

  defp asset_result(
         %RunnerWork{} = work,
         %Asset{} = asset,
         started_at,
         finished_at,
         status,
         meta,
         error
       ) do
    meta = RuntimeConfigRedactor.redact(meta, asset.runtime_config || %{})
    duration_ms = duration_ms(started_at, finished_at)

    normalized_error = normalize_error(error)

    %RunnerAssetResult{
      ref: asset.ref,
      status: status,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: duration_ms,
      meta: meta,
      error: normalized_error,
      attempt_count: work.attempt,
      max_attempts: work.max_attempts,
      attempts: [
        %{
          attempt: work.attempt,
          started_at: started_at,
          finished_at: finished_at,
          duration_ms: duration_ms,
          status: status,
          meta: meta,
          error: normalized_error
        }
      ],
      asset_step_id: work.asset_step_id
    }
  end

  defp normalize_error(nil), do: nil
  defp normalize_error(%RunnerError{} = error), do: error
  defp normalize_error(error), do: RunnerError.normalize(error)

  defp duration_ms(started_at, finished_at) do
    max(DateTime.diff(finished_at, started_at, :millisecond), 0)
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

  defp emit_log(
         server,
         execution_id,
         %RunnerWork{} = work,
         %Asset{} = asset,
         sequence,
         level,
         message,
         extra_metadata \\ %{}
       ) do
    producer_id = "runner:" <> execution_id

    metadata =
      work
      |> RunnerWork.lifecycle_metadata()
      |> Map.put(:runner_execution_id, execution_id)
      |> Map.merge(extra_metadata)

    LogSink.emit(server, execution_id, %{
      source: :runner,
      level: level,
      message: message,
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      asset_ref: asset.ref,
      runner_execution_id: execution_id,
      attempt: work.attempt,
      metadata: metadata,
      occurred_at: DateTime.utc_now(),
      producer_id: producer_id,
      producer_sequence: sequence
    })
  end

  defp execute_sql_asset(%Asset{} = asset, %Version{} = version, %RunnerWork{} = work) do
    SQLAssetRuntime.run_manifest(asset, version, work)
  rescue
    error ->
      {:error, RunnerError.exception(:error, error, __STACKTRACE__)}
  end
end

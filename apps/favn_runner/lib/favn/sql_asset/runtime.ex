defmodule Favn.SQLAsset.Runtime do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.Run.Context
  alias Favn.SQL.Client, as: SQLClient

  alias Favn.SQL.{Explain, IncrementalWindow, MaterializationResult, Params, Preview, Render}

  alias Favn.SQLAsset.{Compiler, Definition, Error, Renderer}
  alias Favn.Window.Runtime
  alias FavnRunner.SQL.MaterializationPlanner

  @runner_registry FavnRunner.ConnectionRegistry

  @type opts :: [params: map(), runtime: map(), timeout_ms: pos_integer()]

  @spec run_manifest(Asset.t(), Version.t(), RunnerWork.t()) :: {:ok, map()} | {:error, Error.t()}
  def run_manifest(%Asset{} = asset, %Version{} = version, work) do
    opts = [
      params: Map.get(work, :params, %{}),
      runtime: trigger_runtime(Map.get(work, :trigger, %{}))
    ]

    with {:ok, %Definition{} = definition} <- manifest_definition(asset, version),
         {:ok, %Render{} = rendered} <- render_for_materialize(definition, opts),
         {:ok, write_plan, result} <- materialize_render(definition, rendered, opts) do
      {:ok,
       %{
         materialized: write_plan.materialization,
         connection: rendered.connection,
         rows_affected: result.rows_affected,
         command: result.command
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @spec render(map(), opts()) :: {:ok, Render.t()} | {:error, Error.t()}
  def render(asset, opts \\ [])

  def render(%{type: :sql, module: module}, opts) when is_atom(module) and is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         {:ok, %Render{} = rendered} <- Renderer.render(definition, opts) do
      {:ok, rendered}
    end
  end

  def render(%{} = asset, _opts) do
    asset_ref = Map.get(asset, :ref)

    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :render,
       asset_ref: asset_ref,
       message: "asset #{inspect(asset_ref)} is not a SQL asset"
     }}
  end

  @spec preview(map(), opts()) :: {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%{} = asset, opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit, 100)

    with :ok <- validate_limit(limit),
         {:ok, %Render{} = rendered} <- render(asset, opts),
         statement <- preview_statement(rendered.sql, limit),
         {:ok, result} <- query_render(rendered, statement, :preview, opts) do
      {:ok, %Preview{render: rendered, limit: limit, statement: statement, result: result}}
    end
  end

  @spec explain(map(), opts()) :: {:ok, Explain.t()} | {:error, Error.t()}
  def explain(%{} = asset, opts \\ []) when is_list(opts) do
    analyze? = Keyword.get(opts, :analyze?, false)

    with {:ok, %Render{} = rendered} <- render(asset, opts),
         statement <- explain_statement(rendered.sql, analyze?),
         {:ok, result} <- query_render(rendered, statement, :explain, opts) do
      {:ok, %Explain{render: rendered, statement: statement, analyze?: analyze?, result: result}}
    end
  end

  @spec materialize(map(), opts()) :: {:ok, MaterializationResult.t()} | {:error, Error.t()}
  def materialize(%{type: :sql, module: module}, opts) when is_atom(module) and is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         {:ok, %Render{} = rendered} <- render_for_materialize(definition, opts),
         {:ok, write_plan, result} <- materialize_render(definition, rendered, opts) do
      {:ok, %MaterializationResult{render: rendered, write_plan: write_plan, result: result}}
    end
  end

  def materialize(%{} = asset, _opts) do
    asset_ref = Map.get(asset, :ref)

    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :materialize,
       asset_ref: asset_ref,
       message: "asset #{inspect(asset_ref)} is not a SQL asset"
     }}
  end

  @spec run(module(), Context.t()) :: {:ok, map()} | {:error, Error.t()}
  def run(module, %Context{} = ctx) when is_atom(module) do
    with {:ok, %Definition{asset: asset}} <- fetch_definition(module),
         opts <- run_opts(ctx),
         {:ok, %MaterializationResult{} = output} <- materialize(asset, opts) do
      {:ok,
       %{
         materialized: output.write_plan.materialization,
         connection: output.render.connection,
         rows_affected: output.result.rows_affected,
         command: output.result.command
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp run_opts(%Context{} = ctx) do
    runtime =
      case ctx.window do
        nil -> %{}
        window -> %{window: window}
      end

    [params: ctx.params || %{}, runtime: runtime]
  end

  defp fetch_definition(module) do
    case Compiler.fetch_definition(module) do
      {:ok, %Definition{} = definition} ->
        {:ok, definition}

      {:error, reason} ->
        {:error,
         %Error{
           type: :invalid_sql_asset_definition,
           phase: :render,
           message: "failed to load SQL asset definition for #{inspect(module)}",
           details: %{module: module, reason: reason}
         }}
    end
  end

  defp validate_limit(limit) when is_integer(limit) and limit > 0, do: :ok

  defp validate_limit(limit) do
    {:error,
     %Error{
       type: :binding_failure,
       phase: :preview,
       message: "preview limit must be a positive integer",
       details: %{limit: limit}
     }}
  end

  defp preview_statement(sql, limit) do
    "SELECT * FROM (#{trim_sql(sql)}) AS favn_preview LIMIT #{limit}"
  end

  defp explain_statement(sql, true), do: "EXPLAIN ANALYZE #{trim_sql(sql)}"
  defp explain_statement(sql, false), do: "EXPLAIN #{trim_sql(sql)}"

  defp query_render(%Render{} = rendered, statement, phase, opts) do
    with_session(rendered.connection, opts, fn session ->
      SQLClient.query(session, statement, params: adapter_params(rendered.params))
    end)
    |> map_sql_result_error(rendered.asset_ref, phase)
  end

  defp materialize_render(%Definition{} = definition, %Render{} = rendered, opts) do
    with_session(rendered.connection, opts, fn session ->
      with {:ok, write_plan} <- MaterializationPlanner.build(session, definition, rendered),
           {:ok, result} <-
             SQLClient.materialize(session, write_plan, params: adapter_params(rendered.params)) do
        {:ok, write_plan, result}
      end
    end)
    |> map_sql_result_error(rendered.asset_ref, :materialize)
  end

  defp render_for_materialize(
         %Definition{materialization: {:incremental, _opts}} = definition,
         opts
       ) do
    with {:ok, %Render{} = initial_render} <- Renderer.render(definition, opts),
         {:ok, %Runtime{} = runtime_window} <- runtime_window(initial_render, definition),
         {:ok, %IncrementalWindow{} = effective_window} <-
           IncrementalWindow.resolve(runtime_window, definition.asset.window_spec),
         {:ok, %Runtime{} = effective_runtime} <- IncrementalWindow.to_runtime(effective_window),
         {:ok, %Render{} = widened_render} <-
           Renderer.render(definition, put_runtime_window(opts, effective_runtime)) do
      {:ok, widened_render}
    end
  end

  defp render_for_materialize(%Definition{} = definition, opts),
    do: Renderer.render(definition, opts)

  defp runtime_window(%Render{} = render, %Definition{} = definition) do
    case render.runtime do
      %Runtime{} = window ->
        {:ok, window}

      _ ->
        {:error,
         %Error{
           type: :materialization_planning_failed,
           phase: :materialize,
           asset_ref: render.asset_ref,
           message: "incremental materialization requires runtime.window",
           details: %{materialization: definition.materialization}
         }}
    end
  end

  defp put_runtime_window(opts, %Runtime{} = runtime_window) do
    runtime_map =
      opts
      |> Keyword.get(:runtime, %{})
      |> case do
        map when is_map(map) -> map
        _ -> %{}
      end

    Keyword.put(opts, :runtime, Map.put(runtime_map, :window, runtime_window))
  end

  defp with_session(connection, opts, fun) when is_function(fun, 1) do
    timeout_opts =
      case Keyword.get(opts, :timeout_ms) do
        timeout when is_integer(timeout) and timeout > 0 -> [timeout_ms: timeout]
        _ -> []
      end

    with {:ok, session} <-
           SQLClient.connect(
             connection,
             Keyword.put(timeout_opts, :registry_name, @runner_registry)
           ) do
      try do
        fun.(session)
      after
        _ = SQLClient.disconnect(session)
      end
    end
  rescue
    error -> {:error, error}
  end

  defp map_sql_result_error({:ok, result}, _asset_ref, _phase), do: {:ok, result}

  defp map_sql_result_error({:ok, write_plan, result}, _asset_ref, _phase),
    do: {:ok, write_plan, result}

  defp map_sql_result_error({:error, %Favn.SQL.Error{} = error}, asset_ref, phase) do
    {:error,
     %Error{
       type: :backend_execution_failed,
       phase: phase,
       asset_ref: asset_ref,
       message: error.message || "SQL execution failed",
       details: %{connection: error.connection, operation: error.operation},
       cause: error
     }}
  end

  defp map_sql_result_error({:error, %Error{} = error}, _asset_ref, _phase), do: {:error, error}

  defp map_sql_result_error({:error, reason}, asset_ref, phase) do
    {:error,
     %Error{
       type: :backend_execution_failed,
       phase: phase,
       asset_ref: asset_ref,
       message: "SQL execution failed",
       cause: reason
     }}
  end

  defp trim_sql(sql) when is_binary(sql) do
    sql
    |> String.trim()
    |> String.trim_trailing(";")
    |> String.trim()
  end

  defp adapter_params(%Params{} = params), do: Params.to_adapter_params(params)

  defp manifest_definition(
         %Asset{type: :sql, sql_execution: %SQLExecution{} = payload} = asset,
         %Version{} = version
       ) do
    asset_stub = %{
      module: asset.module,
      name: asset.name,
      entrypoint: :asset,
      ref: asset.ref,
      arity: 1,
      type: :sql,
      file: "manifest",
      line: 1,
      config: asset.config || %{},
      window_spec: asset.window,
      relation: asset.relation,
      materialization: asset.materialization,
      relation_inputs: asset.relation_inputs || []
    }

    {:ok,
     %Definition{
       module: asset.module,
       asset: asset_stub,
       sql: payload.sql,
       template: payload.template,
       materialization: asset.materialization,
       relation_inputs: asset.relation_inputs || [],
       sql_definitions: payload.sql_definitions,
       raw_asset: %{
         manifest_relation_by_module: relation_map(version),
         deferred_resolution: :manifest_only
       }
     }}
  end

  defp manifest_definition(%Asset{} = asset, _version) do
    {:error,
     %Error{
       type: :invalid_sql_asset_definition,
       phase: :runtime,
       asset_ref: asset.ref,
       message: "manifest SQL execution payload is missing",
       details: %{asset_ref: asset.ref}
     }}
  end

  defp relation_map(%Version{manifest: %{assets: assets}}) when is_list(assets) do
    assets
    |> Enum.reduce(%{}, fn
      %Asset{module: module, relation: %RelationRef{} = relation}, acc
      when is_atom(module) ->
        Map.put(acc, module, relation)

      _, acc ->
        acc
    end)
  end

  defp relation_map(_), do: %{}

  defp trigger_runtime(%{window: %Runtime{} = window}), do: %{window: window}
  defp trigger_runtime(_), do: %{}
end

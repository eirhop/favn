defmodule Favn.SQLAsset.Runtime do
  @moduledoc false

  alias Favn.Asset
  alias Favn.Run.Context
  alias Favn.SQL
  alias Favn.SQL.{Explain, MaterializationPlanner, MaterializationResult, Params, Preview, Render}
  alias Favn.SQLAsset.{Compiler, Definition, Error, Renderer}

  @type opts :: [params: map(), runtime: map(), timeout_ms: pos_integer()]

  @spec render(Asset.t(), opts()) :: {:ok, Render.t()} | {:error, Error.t()}
  def render(asset, opts \\ [])

  def render(%Asset{type: :sql, module: module}, opts) when is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         {:ok, %Render{} = rendered} <- Renderer.render(definition, opts) do
      {:ok, rendered}
    end
  end

  def render(%Asset{} = asset, _opts) do
    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :render,
       asset_ref: asset.ref,
       message: "asset #{inspect(asset.ref)} is not a SQL asset"
     }}
  end

  @spec preview(Asset.t(), opts()) :: {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%Asset{} = asset, opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit, 100)

    with :ok <- validate_limit(limit),
         {:ok, %Render{} = rendered} <- render(asset, opts),
         statement <- preview_statement(rendered.sql, limit),
         {:ok, result} <- query_render(rendered, statement, :preview, opts) do
      {:ok, %Preview{render: rendered, limit: limit, statement: statement, result: result}}
    end
  end

  @spec explain(Asset.t(), opts()) :: {:ok, Explain.t()} | {:error, Error.t()}
  def explain(%Asset{} = asset, opts \\ []) when is_list(opts) do
    analyze? = Keyword.get(opts, :analyze?, false)

    with {:ok, %Render{} = rendered} <- render(asset, opts),
         statement <- explain_statement(rendered.sql, analyze?),
         {:ok, result} <- query_render(rendered, statement, :explain, opts) do
      {:ok, %Explain{render: rendered, statement: statement, analyze?: analyze?, result: result}}
    end
  end

  @spec materialize(Asset.t(), opts()) :: {:ok, MaterializationResult.t()} | {:error, Error.t()}
  def materialize(%Asset{type: :sql, module: module} = _asset, opts) when is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         {:ok, %Render{} = rendered} <- Renderer.render(definition, opts),
         {:ok, write_plan, result} <- materialize_render(definition, rendered, opts) do
      {:ok, %MaterializationResult{render: rendered, write_plan: write_plan, result: result}}
    end
  end

  def materialize(%Asset{} = asset, _opts) do
    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :materialize,
       asset_ref: asset.ref,
       message: "asset #{inspect(asset.ref)} is not a SQL asset"
     }}
  end

  @spec run(module(), Context.t()) :: Asset.return_value()
  def run(module, %Context{} = ctx) when is_atom(module) do
    with {:ok, %Definition{asset: %Asset{} = asset}} <- fetch_definition(module),
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
      SQL.query(session, statement, params: adapter_params(rendered.params))
    end)
    |> map_sql_result_error(rendered.asset_ref, phase)
  end

  defp materialize_render(%Definition{} = definition, %Render{} = rendered, opts) do
    with_session(rendered.connection, opts, fn session ->
      with {:ok, write_plan} <- MaterializationPlanner.build(session, definition, rendered),
           {:ok, result} <-
             SQL.materialize(session, write_plan, params: adapter_params(rendered.params)) do
        {:ok, write_plan, result}
      end
    end)
    |> map_sql_result_error(rendered.asset_ref, :materialize)
  end

  defp with_session(connection, opts, fun) when is_function(fun, 1) do
    timeout_opts =
      case Keyword.get(opts, :timeout_ms) do
        timeout when is_integer(timeout) and timeout > 0 -> [timeout_ms: timeout]
        _ -> []
      end

    with {:ok, session} <- SQL.connect(connection, timeout_opts) do
      try do
        fun.(session)
      after
        _ = SQL.disconnect(session)
      end
    end
  rescue
    error -> {:error, error}
  end

  defp map_sql_result_error({:ok, result}, _asset_ref, _phase), do: {:ok, result}

  defp map_sql_result_error({:ok, write_plan, result}, _asset_ref, _phase),
    do: {:ok, write_plan, result}

  defp map_sql_result_error({:error, %SQL.Error{} = error}, asset_ref, phase) do
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
end

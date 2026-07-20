defmodule FavnRunner.SQL.MaterializationPlanner do
  @moduledoc """
  Runner-owned SQL asset materialization planner.

  The planner lives in `favn_runner` because it translates runner SQL asset
  render output into the shared `%Favn.SQL.WritePlan{}` adapter contract.
  """

  alias Favn.RelationRef
  alias Favn.SQL.{Client, Params, Render, Session, WritePlan}
  alias Favn.SQLAsset.{Definition, Error}
  alias Favn.Window.Runtime

  @typedoc "Authoritative source used to inspect incremental output columns."
  @type column_source :: :query | {:relation, RelationRef.t()}

  @doc """
  Builds the adapter write plan for a rendered SQL asset.

  A staged relation may be supplied as the authoritative column source when
  checked materialization has already created an inspectable candidate table.
  """
  @spec build(Session.t(), Definition.t(), Render.t(), column_source()) ::
          {:ok, WritePlan.t()} | {:error, Error.t()}
  def build(
        %Session{} = session,
        %Definition{} = definition,
        %Render{} = render,
        column_source \\ :query
      ) do
    case render.materialization do
      :view ->
        {:ok, view_write_plan(render)}

      :table ->
        {:ok, table_write_plan(render)}

      {:incremental, opts} ->
        build_incremental(session, definition, render, opts, column_source)
    end
  end

  defp build_incremental(
         %Session{} = session,
         %Definition{} = definition,
         %Render{} = render,
         opts,
         column_source
       ) do
    strategy = Keyword.fetch!(opts, :strategy)

    with {:ok, %Runtime{} = runtime_window} <- runtime_window(definition, render),
         {:ok, target_exists?} <- target_exists?(session, render),
         {:ok, _} <-
           validate_strategy_shape(strategy, opts, session, render, column_source),
         {:ok, _} <-
           maybe_validate_existing_target(strategy, opts, session, render, target_exists?) do
      if target_exists? do
        incremental_write_plan(render, strategy, opts, runtime_window)
      else
        bootstrap_write_plan(render, strategy, runtime_window)
      end
    end
  end

  defp bootstrap_write_plan(%Render{} = render, strategy, %Runtime{} = window) do
    {:ok,
     table_write_plan(render)
     |> Map.merge(%{
       materialization: :incremental,
       strategy: strategy,
       mode: :bootstrap,
       transactional?: false,
       window: window,
       effective_window: window,
       bootstrap?: true,
       metadata: %{
         mode: :bootstrap,
         bootstrap?: true,
         strategy: strategy,
         effective_window: window
       }
     })}
  end

  defp incremental_write_plan(%Render{} = render, :append, _opts, %Runtime{} = window) do
    {:ok,
     %WritePlan{
       asset_ref: render.asset_ref,
       connection: render.connection,
       materialization: :incremental,
       strategy: :append,
       mode: :incremental,
       target: relation(render, :table),
       query: render,
       select_sql: render.sql,
       params: render.params,
       transactional?: false,
       window: window,
       effective_window: window,
       bootstrap?: false,
       metadata: %{mode: :incremental, strategy: :append, effective_window: window}
     }}
  end

  defp incremental_write_plan(
         %Render{} = render,
         :delete_insert,
         opts,
         %Runtime{} = window
       ) do
    column = opts |> Keyword.fetch!(:window_column) |> normalize_column_name()

    {:ok,
     %WritePlan{
       asset_ref: render.asset_ref,
       connection: render.connection,
       materialization: :incremental,
       strategy: :delete_insert,
       mode: :incremental,
       target: relation(render, :table),
       query: render,
       select_sql: render.sql,
       params: render.params,
       transactional?: true,
       window: window,
       effective_window: window,
       window_column: column,
       bootstrap?: false,
       options: %{window_column: column},
       metadata: %{
         mode: :incremental,
         strategy: :delete_insert,
         effective_window: window,
         delete_scope: %{window_column: column, predicate: :half_open}
       }
     }}
  end

  defp runtime_window(%Definition{} = definition, %Render{} = render) do
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

  defp target_exists?(%Session{} = session, %Render{} = render) do
    case Client.relation(session, target_ref(render)) do
      {:ok, nil} -> {:ok, false}
      {:ok, _relation} -> {:ok, true}
      {:error, reason} -> planning_error(render, "failed to inspect incremental target", reason)
    end
  end

  defp validate_strategy_shape(:append, _opts, _session, _render, _column_source),
    do: {:ok, :append}

  defp validate_strategy_shape(
         :delete_insert,
         opts,
         %Session{} = session,
         %Render{} = render,
         column_source
       ) do
    column = opts |> Keyword.fetch!(:window_column) |> normalize_column_name()

    with :ok <- ensure_transaction_support(session, render),
         {:ok, columns} <- rendered_columns(session, render, column_source),
         :ok <- ensure_column_present(columns, column, render, source_name(column_source)) do
      {:ok, :delete_insert}
    end
  end

  defp validate_strategy_shape(
         strategy,
         _opts,
         _session,
         %Render{} = render,
         _column_source
       ) do
    {:error,
     %Error{
       type: :unsupported_materialization,
       phase: :materialize,
       asset_ref: render.asset_ref,
       message: "incremental strategy #{inspect(strategy)} is not supported in Phase 4b",
       details: %{strategy: strategy}
     }}
  end

  defp maybe_validate_existing_target(:append, _opts, _session, _render, _target_exists?),
    do: {:ok, :ok}

  defp maybe_validate_existing_target(
         :delete_insert,
         opts,
         %Session{} = session,
         %Render{} = render,
         true
       ) do
    column = opts |> Keyword.fetch!(:window_column) |> normalize_column_name()

    with {:ok, columns} <- Client.columns(session, target_ref(render)),
         :ok <- ensure_column_present(Enum.map(columns, & &1.name), column, render, :target) do
      {:ok, :ok}
    else
      {:error, reason} ->
        planning_error(render, "failed to inspect incremental target columns", reason)
    end
  end

  defp maybe_validate_existing_target(:delete_insert, _opts, _session, _render, false),
    do: {:ok, :ok}

  defp ensure_transaction_support(%Session{capabilities: %{transactions: :supported}}, _render),
    do: :ok

  defp ensure_transaction_support(%Session{} = session, %Render{} = render) do
    {:error,
     %Error{
       type: :unsupported_materialization,
       phase: :materialize,
       asset_ref: render.asset_ref,
       message: "incremental :delete_insert requires transactional adapter support",
       details: %{connection: session.resolved.name}
     }}
  end

  defp rendered_columns(%Session{} = session, %Render{} = render, :query) do
    statement = "SELECT * FROM (#{trim_sql(render.sql)}) AS favn_incremental_probe LIMIT 0"

    case Client.query(session, statement, params: adapter_params(render.params)) do
      {:ok, %Favn.SQL.Result{columns: []}} ->
        inspection_unavailable_error(render, :query)

      {:ok, %Favn.SQL.Result{columns: columns}} ->
        {:ok, columns}

      {:error, reason} ->
        planning_error(render, "failed to inspect rendered incremental columns", reason)
    end
  end

  defp rendered_columns(
         %Session{} = session,
         %Render{} = render,
         {:relation, %RelationRef{} = relation_ref}
       ) do
    case Client.columns(session, relation_ref) do
      {:ok, []} ->
        inspection_unavailable_error(render, :relation)

      {:ok, columns} when is_list(columns) ->
        {:ok, Enum.map(columns, & &1.name)}

      {:error, reason} ->
        planning_error(render, "failed to inspect incremental source relation columns", reason)
    end
  end

  defp source_name(:query), do: :query
  defp source_name({:relation, %RelationRef{}}), do: :relation

  defp inspection_unavailable_error(%Render{} = render, source) do
    {:error,
     %Error{
       type: :materialization_planning_failed,
       phase: :materialize,
       asset_ref: render.asset_ref,
       message: "incremental source column metadata is unavailable",
       details: %{source: source, columns: []}
     }}
  end

  defp ensure_column_present(columns, column, %Render{} = render, source) when is_list(columns) do
    if Enum.any?(columns, &(normalize_column_name(&1) == column)) do
      :ok
    else
      {:error,
       %Error{
         type: :materialization_planning_failed,
         phase: :materialize,
         asset_ref: render.asset_ref,
         message: "incremental delete scope column is missing",
         details: %{window_column: column, source: source, columns: columns}
       }}
    end
  end

  defp planning_error(%Render{} = render, message, reason) do
    {:error,
     %Error{
       type: :materialization_planning_failed,
       phase: :materialize,
       asset_ref: render.asset_ref,
       message: message,
       cause: reason
     }}
  end

  defp target_ref(%Render{} = render) do
    RelationRef.new!(%{
      catalog: render.relation.catalog,
      schema: render.relation.schema,
      name: render.relation.name
    })
  end

  defp relation(%Render{} = render, type) do
    %Favn.SQL.Relation{
      catalog: render.relation.catalog,
      schema: render.relation.schema,
      name: render.relation.name,
      type: type,
      metadata: %{}
    }
  end

  defp view_write_plan(%Render{} = render) do
    %WritePlan{
      asset_ref: render.asset_ref,
      connection: render.connection,
      materialization: :view,
      target: relation(render, :view),
      query: render,
      select_sql: render.sql,
      params: render.params,
      replace_existing?: true,
      replace?: true,
      metadata: %{create_or_replace?: true}
    }
  end

  defp table_write_plan(%Render{} = render) do
    %WritePlan{
      asset_ref: render.asset_ref,
      connection: render.connection,
      materialization: :table,
      target: relation(render, :table),
      query: render,
      select_sql: render.sql,
      params: render.params,
      replace_existing?: true,
      replace?: true,
      metadata: %{rebuild?: true}
    }
  end

  defp trim_sql(sql) when is_binary(sql) do
    sql
    |> String.trim()
    |> String.trim_trailing(";")
    |> String.trim()
  end

  defp adapter_params(%Params{} = params), do: Params.to_adapter_params(params)

  defp normalize_column_name(column) when is_atom(column), do: Atom.to_string(column)
  defp normalize_column_name(column) when is_binary(column), do: column
end

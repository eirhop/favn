defmodule FavnAuthoring.Semantic.DSL do
  @moduledoc false

  alias Favn.SQL

  @metric_options [:unit, :description, :format, :time_aggregate, :minimum_grain, :file]

  def capture!(name, body, env) do
    if Module.get_attribute(env.module, :favn_semantic),
      do: fail!(env, "only one semantic block is allowed per SQL asset")

    if Module.get_attribute(env.module, :favn_query_captured),
      do: fail!(env, "semantic must appear before query")

    if Module.get_attribute(env.module, :favn_sql_contracts) in [nil, []],
      do: fail!(env, "semantic requires a preceding output contract")

    name = name!(name, env)

    model = %{
      name: name,
      module: env.module,
      file: env.file,
      line: env.line,
      dimension: nil,
      time: nil,
      hierarchies: [],
      metrics: []
    }

    statements =
      case body do
        {:__block__, _, entries} -> entries
        entry -> [entry]
      end

    model = Enum.reduce(statements, model, &declaration!(&1, &2, env))
    Module.put_attribute(env.module, :favn_semantic, model)
    :ok
  end

  defp declaration!({:dimension, meta, [name, opts]}, model, env) do
    env = at(env, meta)
    if model.dimension, do: fail!(env, "duplicate dimension declaration")
    opts = options!(opts, [:label], env)
    label = name!(Keyword.fetch!(opts, :label), env)
    %{model | dimension: %{name: name!(name, env), label: label}}
  end

  defp declaration!({:hierarchy, meta, [name, columns]}, model, env) do
    env = at(env, meta)
    entry = %{name: name!(name, env), columns: literal!(columns, env)}
    %{model | hierarchies: model.hierarchies ++ [entry]}
  end

  defp declaration!({:time, meta, [column, opts]}, model, env) do
    env = at(env, meta)
    if model.time, do: fail!(env, "duplicate time declaration")
    opts = options!(opts, [:grain, :timezone], env)

    entry = %{
      column: name!(column, env),
      grain: Keyword.fetch!(opts, :grain),
      timezone: Keyword.fetch!(opts, :timezone)
    }

    %{model | time: entry}
  end

  defp declaration!({:metric, meta, [head, opts, [do: body]]}, model, env) do
    metric!(head, opts, body, model, at(env, meta))
  end

  defp declaration!({:metric, meta, [head, opts]}, model, env) do
    metric!(head, opts, nil, model, at(env, meta))
  end

  defp declaration!(_, _, env),
    do: fail!(env, "semantic accepts only dimension, hierarchy, time, and metric declarations")

  defp metric!({name, _, args}, opts_ast, body, model, env) when is_list(args) do
    name = name!(name, env)

    args =
      Enum.map(args, fn
        {arg, _, context} when is_atom(context) -> name!(arg, env)
        _ -> fail!(env, "metric inputs must be source column names")
      end)

    opts = options!(opts_ast, @metric_options, env)
    file = Keyword.get(opts, :file)

    {sql, source_file, line} =
      case {body, file} do
        {nil, path} when is_binary(path) ->
          path = Path.expand(path, Path.dirname(env.file))
          Module.put_attribute(env.module, :external_resource, path)

          case File.read(path) do
            {:ok, sql} -> {sql, path, 1}
            {:error, _} -> fail!(env, "cannot read metric SQL file #{path}")
          end

        {nil, _} ->
          fail!(env, "metric requires a ~SQL body or file")

        {_, nil} ->
          {SQL.extract_sql!(body, env, "metric body must be a literal ~SQL expression"), env.file,
           env.line}

        _ ->
          fail!(env, "metric body and file are mutually exclusive")
      end

    metric = %{
      name: name,
      args: args,
      sql: sql,
      opts: Keyword.delete(opts, :file),
      file: source_file,
      line: line
    }

    %{model | metrics: model.metrics ++ [metric]}
  end

  defp metric!(_, _, _, _, env), do: fail!(env, "metric requires a function-style signature")

  defp options!(ast, keys, env) do
    opts = literal!(ast, env)
    unless Keyword.keyword?(opts), do: fail!(env, "semantic options must be literal keywords")
    names = Keyword.keys(opts)

    if names -- keys != [] or length(names) != length(Enum.uniq(names)),
      do: fail!(env, "unknown or duplicate semantic option")

    opts
  end

  defp literal!(ast, env) do
    unless Macro.quoted_literal?(ast),
      do: fail!(env, "semantic declarations require literal values")

    {value, []} = Code.eval_quoted(ast, [], env)
    value
  end

  defp name!(name, env) when is_atom(name) and not is_nil(name) do
    if Regex.match?(~r/^[a-z][a-z0-9_]{0,63}$/, Atom.to_string(name)),
      do: name,
      else: fail!(env, "semantic names must be lowercase ASCII identifiers (max 64 bytes)")
  end

  defp name!(_, env), do: fail!(env, "semantic names must be literal atoms")
  defp at(env, meta), do: %{env | line: Keyword.get(meta, :line, env.line)}

  defp fail!(env, message),
    do: raise(CompileError, file: env.file, line: env.line, description: message)
end

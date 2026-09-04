defmodule FavnTestSupport.SQLPackageFixture do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.{ExecutionPackage, SQLExecution}
  alias Favn.SQL.{Check, Contract, Definition, Template}

  def package(columns, mode \\ :ordinary, indent \\ 4) when columns > 0 and columns <= 20_000 do
    definitions = if mode in [:dynamic, :rich], do: [helper()], else: []
    known = Map.new(definitions, &{Definition.key(&1), &1})

    projection =
      Enum.map_join(1..columns, ",\n", fn index ->
        expression =
          if mode == :dynamic,
            do: "adjusted(@value_#{index})",
            else: "coalesce(metric_#{index}, 0)"

        String.duplicate(" ", indent) <> expression <> " AS value_#{index}"
      end)

    sql = "SELECT\n" <> projection <> "\nFROM warehouse.input\nWHERE day >= @window_start"

    execution = %SQLExecution{
      sql: sql,
      template: compile(sql, known),
      sql_definitions: definitions
    }

    execution = if mode == :rich, do: add_checks_and_scopes(execution, known), else: execution
    {:ok, package} = ExecutionPackage.new({__MODULE__, :asset}, execution)
    package
  end

  def work(package) do
    %RunnerWork{
      run_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      run_started_at: ~U[2026-01-01 00:00:00Z],
      asset_ref: package.asset_ref,
      manifest_version_id: "mv_" <> String.duplicate("a", 64),
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: "rr_" <> String.duplicate("b", 64),
      execution_package: package,
      metadata: %{"fixture" => "sql_package_size"}
    }
  end

  def source_bytes(%ExecutionPackage{sql_execution: execution}) do
    ([execution.sql, execution.incremental_scope_sql, execution.full_scope_sql] ++
       Enum.map(execution.checks, & &1.sql) ++ Enum.map(execution.sql_definitions, & &1.sql))
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&byte_size/1)
    |> Enum.sum()
  end

  def templates(%ExecutionPackage{sql_execution: execution}) do
    [execution.template, execution.incremental_scope_template, execution.full_scope_template]
    |> Kernel.++(Enum.map(execution.checks, & &1.template))
    |> Kernel.++(Enum.map(execution.sql_definitions, & &1.template))
    |> Enum.reject(&is_nil/1)
  end

  def count(value, type) when is_map(value) do
    own = if Map.get(value, :__struct__) == type, do: 1, else: 0
    own + Enum.sum(Enum.map(Map.values(value), &count(&1, type)))
  end

  def count(values, type) when is_list(values), do: Enum.sum(Enum.map(values, &count(&1, type)))
  def count(_value, _type), do: 0

  defp compile(sql, known \\ %{}) do
    Template.compile!(sql, file: "generic_package.sql", line: 1, known_definitions: known)
  end

  defp helper do
    sql = "coalesce(@value, 0)"

    %Definition{
      module: __MODULE__,
      name: :adjusted,
      arity: 1,
      shape: :expression,
      params: [%Definition.Param{name: :value, index: 0}],
      sql: sql,
      template:
        Template.compile!(sql,
          file: "generic_helper.sql",
          line: 1,
          scope: :definition,
          local_args: [:value]
        ),
      file: "generic_helper.sql",
      line: 1,
      declared_file: "generic_helper.ex",
      declared_line: 1
    }
  end

  defp add_checks_and_scopes(execution, known) do
    contract =
      Contract.new!(%{
        columns: [%{name: :value_1, type: :integer, null: false}],
        unique_keys: [[:value_1]],
        row_counts: [[min: 1]]
      })

    generated =
      Enum.map(Contract.generated_check_specs(contract), fn spec ->
        spec
        |> Map.merge(%{
          template: compile(spec.sql),
          origin: :contract,
          uses_query?: true,
          uses_target?: false
        })
        |> Check.new!()
      end)

    sql = "SELECT count(*) = 0 AS passed FROM query() WHERE adjusted(adjusted(value_1)) < 0"

    authored =
      Check.new!(%{
        name: :non_negative,
        at: :before_materialize,
        on_violation: :fail,
        sql: sql,
        template: compile(sql, known),
        uses_query?: true,
        uses_target?: false
      })

    incremental = "SELECT id FROM warehouse.input WHERE day >= @window_start"
    full = "SELECT id FROM warehouse.input"

    %{
      execution
      | contract: contract,
        checks: generated ++ [authored],
        incremental_scope_sql: incremental,
        incremental_scope_template: compile(incremental),
        full_scope_sql: full,
        full_scope_template: compile(full)
    }
  end
end

defmodule Favn.SemanticTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.Semantic.{Artifact, Catalog, Compiler, Snapshot}
  alias Favn.SQL.Contract

  defp assets do
    contract =
      Contract.new!(
        grain: [by: [:sale_id]],
        columns: [
          %{name: :sale_id, type: :integer, null: false},
          %{name: :gross, type: :decimal, null: false},
          %{name: :discount, type: :decimal, null: false},
          %{name: :units, type: :integer, null: false},
          %{name: :sale_date, type: :date, null: false}
        ]
      )

    [
      %{
        ref: {Sales, :asset},
        module: Sales,
        type: :sql,
        depends_on: [],
        relation: RelationRef.new!(schema: "mart", name: "sales"),
        contract: contract
      }
    ]
  end

  defp metric(
         name \\ :revenue,
         args \\ [:gross, :discount],
         sql \\ "SUM(@gross - @discount)",
         opts \\ []
       ) do
    %{
      name: name,
      args: args,
      sql: sql,
      file: "/project/sales.ex",
      line: 20,
      opts:
        Keyword.merge(
          [
            unit: {:currency, "NOK"},
            description: "Net sales revenue",
            time_aggregate: :aggregate
          ],
          opts
        )
    }
  end

  defp model(metrics \\ [metric()]) do
    %{
      name: :sales,
      module: Sales,
      dimension: nil,
      hierarchies: [],
      time: %{column: :sale_date, grain: :day, timezone: "Europe/Oslo"},
      metrics: metrics,
      file: "/project/sales.ex",
      line: 10
    }
  end

  defp validator(sql, inputs) do
    send(self(), {:validated, sql, inputs})

    {:ok,
     %{
       native_type: "DECIMAL(38,2)",
       nullable: :unknown,
       runtime_version: "v1.5.5",
       compiler_version: "test-validator-v1",
       validation_profile: Map.new(inputs, &{&1.name, "DECIMAL(18,2)"})
     }}
  end

  defp compile!(model \\ model(), assets \\ assets()) do
    assert {:ok, artifact} = Compiler.compile([model], assets, &validator/2)
    artifact
  end

  test "compiler binds source columns in declared order and publishes conservative native evidence" do
    artifact = compile!()
    [model] = artifact.models
    [metric] = model["metrics"]
    assert metric["canonical_sql"] == "SUM(\"gross\" - \"discount\")"
    assert Enum.map(metric["inputs"], & &1["column"]) == ["gross", "discount"]
    assert metric["column_dependencies"] == ["gross", "discount"]
    assert metric["metric_dependencies"] == []
    assert metric["logical_result_type"] == "unknown"
    assert metric["nullable"] == "unknown"
    assert metric["validation"]["validation_result_type"] == "DECIMAL(38,2)"
    assert metric["validation"]["runtime_version"] == "v1.5.5"

    assert_receive {:validated, _,
                    [%{name: "gross", type: :decimal, nullable: false}, %{name: "discount"}]}
  end

  test "composition expands aggregate expressions and validates each dependency only once" do
    leaf = metric()
    units = metric(:sold_units, [:units], "SUM(@units)", unit: :count)

    price =
      metric(
        :unit_price,
        [:gross, :discount, :units],
        "revenue(@gross, @discount) / NULLIF(sold_units(@units), 0)", unit: {:custom, "NOK/unit"})

    artifact = compile!(model([price, leaf, units]))
    compiled = Enum.find(hd(artifact.models)["metrics"], &(&1["name"] == "unit_price"))

    assert compiled["canonical_sql"] ==
             "(SUM(\"gross\" - \"discount\")) / NULLIF((SUM(\"units\")), 0)"

    assert compiled["metric_dependencies"] == ["sales.revenue", "sales.sold_units"]
    assert compiled["column_dependencies"] == []
    assert_receive {:validated, _, _}
    assert_receive {:validated, _, _}
    assert_receive {:validated, _, _}
    refute_receive {:validated, _, _}
  end

  test "composition rejects swapped inputs, expressions, cycles and raw aggregates" do
    cases = [
      {metric(:derived, [:gross, :discount], "revenue(@discount, @gross)"), :invalid_metric_call},
      {metric(:derived, [:gross, :discount], "revenue(@gross + 1, @discount)"),
       :invalid_metric_call},
      {metric(:derived, [:gross, :discount], "SUM(revenue(@gross, @discount))"),
       :mixed_composition},
      {metric(:derived, [:gross, :discount], "revenue(@gross, @discount) + @gross"),
       :mixed_composition}
    ]

    for {invalid, code} <- cases do
      assert {:error, [%{code: ^code}]} =
               Compiler.compile([model([invalid, metric()])], assets(), &validator/2)
    end

    cycle = metric(:revenue, [:gross, :discount], "other(@gross, @discount)")
    other = metric(:other, [:gross, :discount], "revenue(@gross, @discount)")

    assert {:error, [%{code: :metric_cycle}]} =
             Compiler.compile([model([cycle, other])], assets(), &validator/2)
  end

  test "declarations fail explicitly for bare, unused, undeclared and oversized inputs" do
    cases = [
      {metric(:revenue, [:gross], "SUM(gross)"), :bare_column},
      {metric(:revenue, [:gross], "SUM(\"gross\")"), :bare_column},
      {metric(:revenue, [:gross, :discount], "SUM(@gross)"), :unused_argument},
      {metric(:revenue, [:gross], "SUM(@missing)"), :invalid_declaration},
      {metric(:revenue, [:unknown], "SUM(@unknown)"), :unknown_column},
      {metric(:revenue, [], "COUNT(1)"), :invalid_arguments},
      {metric(:revenue, [:gross], String.duplicate(" ", 16_385)), :formula_limit},
      {metric(:sum, [:gross], "SUM(@gross)"), :ambiguous_metric},
      {metric(:revenue, [:gross], "SUM(@gross)", hidden: true), :unknown_option},
      {metric(:revenue, [:gross], "SUM(@gross)", description: ""), :description_required}
    ]

    for {invalid, code} <- cases do
      assert {:error, [%{code: ^code}]} =
               Compiler.compile([model([invalid])], assets(), &validator/2)
    end
  end

  test "native failures preserve safe reason and source while suppressing arbitrary payloads" do
    assert {:error, [%{code: :semantic_driver_unavailable, file: "/project/sales.ex", line: 20}]} =
             Compiler.compile([model()], assets(), fn _, _ ->
               {:error, :semantic_driver_unavailable}
             end)

    assert {:error, [%{code: :native_validation_failed, message: message}]} =
             Compiler.compile([model()], assets(), fn _, _ ->
               {:error, %{secret: "do-not-leak"}}
             end)

    refute message =~ "do-not-leak"
  end

  test "first and last require DATE time in contract grain and publish entity selection" do
    [asset] = assets()
    contract = %{asset.contract | grain: Favn.SQL.Contract.Grain.new!(by: [:sale_id, :sale_date])}
    first = metric(:opening, [:gross], "SUM(@gross)", time_aggregate: :first)
    artifact = compile!(model([first]), [%{asset | contract: contract}])
    [metric] = hd(artifact.models)["metrics"]
    assert metric["entity_key"] == ["sale_id"]
    assert metric["time_aggregate"] == "first"

    assert {:error, [%{code: :time_grain_required}]} =
             Compiler.compile([model([first])], assets(), &validator/2)

    assert {:error, [%{code: :time_required}]} =
             Compiler.compile([%{model([first]) | time: nil}], assets(), &validator/2)
  end

  test "composed selection rules are inherited and cannot mix opening and closing" do
    [asset] = assets()

    asset = %{
      asset
      | contract: %{
          asset.contract
          | grain: Favn.SQL.Contract.Grain.new!(by: [:sale_id, :sale_date])
        }
    }

    opening = metric(:opening, [:gross], "SUM(@gross)", time_aggregate: :first)
    closing = metric(:closing, [:discount], "SUM(@discount)", time_aggregate: :last)
    derived = metric(:difference, [:gross, :discount], "opening(@gross) - closing(@discount)")

    assert {:error, [%{code: :incompatible_time_rules}]} =
             Compiler.compile([model([derived, opening, closing])], [asset], &validator/2)

    derived =
      metric(:double_opening, [:gross], "opening(@gross) * 2")
      |> Map.update!(:opts, &Keyword.delete(&1, :time_aggregate))

    artifact = compile!(model([derived, opening]), [asset])

    assert Enum.find(hd(artifact.models)["metrics"], &(&1["name"] == "double_opening"))[
             "time_aggregate"
           ] == "first"
  end

  test "dimension hierarchy and time metadata enforce complete grain and DATE semantics" do
    dimension = %{
      model([])
      | dimension: %{name: :sale, label: :sale_id},
        hierarchies: [%{name: :calendar_sales, columns: [:sale_date, :sale_id]}]
    }

    assert [%{"dimension" => %{"key" => ["sale_id"]}}] = compile!(dimension).models

    assert {:error, [%{code: :hierarchy_key}]} =
             Compiler.compile(
               [%{dimension | hierarchies: [%{name: :bad, columns: [:sale_id, :sale_date]}]}],
               assets(),
               &validator/2
             )

    assert {:error, [%{code: :invalid_time_grain}]} =
             Compiler.compile([put_in(model().time.grain, :hour)], assets(), &validator/2)

    assert {:error, [%{code: :invalid_time_column}]} =
             Compiler.compile([put_in(model().time.column, :gross)], assets(), &validator/2)

    assert {:error, [%{code: :invalid_timezone}]} =
             Compiler.compile(
               [put_in(model().time.timezone, "Not/A_Zone")],
               assets(),
               &validator/2
             )
  end

  test "identity ignores source locations and discovery ordering, but preserves public argument order" do
    a = compile!()

    relocated = %{
      model()
      | file: "/elsewhere.ex",
        line: 500,
        metrics: [%{metric() | file: "/elsewhere.sql", line: 200}]
    }

    assert compile!(relocated).semantic_version == a.semantic_version
    reversed = metric(:revenue, [:discount, :gross], "SUM(@gross - @discount)")
    refute compile!(model([reversed])).semantic_version == a.semantic_version
    other = %{model() | name: :other}
    assert {:ok, one} = Compiler.compile([model(), other], assets(), &validator/2)
    assert {:ok, two} = Compiler.compile([other, model()], assets(), &validator/2)
    assert one.semantic_version == two.semantic_version
    assert {:ok, json} = Artifact.encode(one)
    assert {:ok, ^one} = Artifact.decode(json)
    refute json =~ "/project"
    refute json =~ "Elixir."
  end

  test "closed codec rejects unknown records, corrupt hashes, malformed metadata and oversized bytes" do
    artifact = compile!()
    {:ok, json} = Artifact.encode(artifact)
    wire = Jason.decode!(json)

    assert {:error, :invalid_semantic_artifact} =
             Artifact.decode(Jason.encode!(Map.put(wire, "unknown", true)))

    assert {:error, :semantic_digest_mismatch} =
             Artifact.decode(
               Jason.encode!(
                 Map.put(wire, "semantic_version", "sm_" <> String.duplicate("0", 64))
               )
             )

    corrupt =
      put_in(
        wire,
        ["models", Access.at(0), "metrics", Access.at(0), "inputs", Access.at(0), "position"],
        5
      )

    assert {:error, :invalid_semantic_artifact} = Artifact.decode(Jason.encode!(corrupt))

    assert {:error, :artifact_too_large} =
             Artifact.decode(String.duplicate("x", 16 * 1024 * 1024 + 1))

    assert {:error, :invalid_json} = Artifact.decode("{")
  end

  @tag :tmp_dir
  test "immutable writes round-trip, are idempotent and reject different existing bytes", %{
    tmp_dir: directory
  } do
    artifact = compile!()
    assert {:ok, output} = Artifact.write(artifact, directory)
    assert {:ok, ^artifact} = Artifact.read(output.path)
    assert {:ok, ^output} = Artifact.write(artifact, directory)
    File.write!(output.path, "changed")
    assert {:error, :immutable_artifact_conflict} = Artifact.write(artifact, directory)
    assert File.ls!(directory) == [artifact.semantic_version]
  end

  test "inspection creates a copyable native macro invocation with no project source lookup" do
    artifact = compile!()
    result = Catalog.inspect(artifact, metric: "sales.revenue", source_alias: "sale\"s")
    [metric] = hd(result["models"])["metrics"]
    assert metric["invocation"] =~ "\"sale\"\"s\".\"gross\", \"sale\"\"s\".\"discount\""
    assert metric["macro_sql"] =~ "AS (SUM(\"gross\" - \"discount\"));"
    assert metric["source_grain"] == ["sale_id"]
    assert metric["compatibility_requirements"]["relation"]["name"] == "sales"
    assert Catalog.inspect(artifact, metric: "missing.metric")["models"] == []
    assert length(Catalog.macros(artifact)) == 1
  end

  test "diff classifies formula changes and propagated composition while descriptions remain informational" do
    old = compile!()

    docs =
      compile!(
        model([
          metric(:revenue, [:gross, :discount], "SUM(@gross - @discount)",
            description: "Corrected business wording"
          )
        ])
      )

    assert [
             %{
               "entity" => "metric:sales.revenue",
               "classification" => "informational",
               "changes" => ["description"]
             }
           ] = Catalog.diff(old, docs)

    changed =
      compile!(model([metric(:revenue, [:gross, :discount], "SUM(@gross) - SUM(@discount)")]))

    assert [%{"classification" => "breaking"}] = Catalog.diff(old, changed)
    assert Catalog.diff(old, old) == []

    assert [%{"classification" => "breaking", "changes" => ["removed"]}] =
             Catalog.diff(old, compile!(model([])))
  end

  test "compatibility consumes columns, logical types, nullability, grain and relation but tolerates unrelated columns" do
    artifact = compile!()
    assert %{status: :unknown} = Catalog.compatible(artifact, nil)
    assert %{status: :compatible} = Catalog.compatible(artifact, artifact.snapshot)
    [asset] = assets()
    extra = Favn.SQL.Contract.Column.new!(:extra, :string, [])

    assert {:ok, snapshot} =
             Snapshot.build([
               %{asset | contract: %{asset.contract | columns: asset.contract.columns ++ [extra]}}
             ])

    assert %{status: :compatible} = Catalog.compatible(artifact, snapshot)

    changed =
      update_in(snapshot, [Access.at(0), "contract", "columns", Access.at(1), "type"], fn _ ->
        "integer"
      end)

    assert %{status: :incompatible, reasons: [%{"reason" => "column_changed:gross"}]} =
             Catalog.compatible(artifact, changed)

    changed = put_in(snapshot, [Access.at(0), "relation", "name"], "renamed_sales")
    assert %{status: :incompatible} = Catalog.compatible(artifact, changed)
    assert %{status: :incompatible} = Catalog.compatible(artifact, [])
  end
end

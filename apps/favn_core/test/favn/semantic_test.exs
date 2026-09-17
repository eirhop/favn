defmodule Favn.SemanticTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.Semantic.{Artifact, Catalog, Compiler, Snapshot}
  alias Favn.SQL.Contract

  defmodule AssetTrap do
    @moduledoc false
    def __favn_single_asset__ do
      send(self(), :customer_asset_loaded)
      true
    end
  end

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
        "revenue(@gross, @discount) / NULLIF(sold_units(@units), 0)",
        unit: {:custom, "NOK/unit"}
      )

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

    for aggregate <- [
          "SUM(1)",
          "\"sum\"(1)",
          "\"SUM\"\n(1)",
          "sum/* comment */(1)",
          "sum/* outer /* nested */ comment */(1)",
          "sum\f(1)",
          "-- comment\rsum(1)"
        ] do
      invalid =
        metric(:derived, [:gross, :discount], "revenue(@gross, @discount) + " <> aggregate)

      assert {:error, [%{code: :mixed_composition}]} =
               Compiler.compile([model([invalid, metric()])], assets(), &validator/2)
    end

    for literal <- ["'\"sum\"(1)'", "E'sum(1)'", "$$\"sum\"(1)$$", "$tag$SUM(1)$tag$"] do
      valid =
        metric(
          :derived,
          [:gross, :discount],
          "CASE WHEN #{literal} = 'text' THEN revenue(@gross, @discount) ELSE 0 END"
        )

      assert {:ok, _} = Compiler.compile([model([valid, metric()])], assets(), &validator/2)
    end

    cycle = metric(:revenue, [:gross, :discount], "other(@gross, @discount)")
    other = metric(:other, [:gross, :discount], "revenue(@gross, @discount)")

    assert {:error, [%{code: :metric_cycle}]} =
             Compiler.compile([model([cycle, other])], assets(), &validator/2)
  end

  test "declarations fail explicitly for bare, unused, undeclared and oversized inputs" do
    cases = [
      {metric(:revenue, [:gross], "SUM(gross)"), :unused_argument},
      {metric(:revenue, [:gross], "SUM(\"gross\")"), :unused_argument},
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
    assert {:error, [%{code: :validator_unavailable}]} =
             Compiler.compile([model()], assets(), Favn.NoSemanticValidatorInstalled)

    assert {:error, [%{code: :semantic_driver_unavailable, file: "/project/sales.ex", line: 20}]} =
             Compiler.compile([model()], assets(), fn _, _ ->
               {:error, :semantic_driver_unavailable}
             end)

    assert {:error, [%{code: :native_validation_failed, message: message}]} =
             Compiler.compile([model()], assets(), fn _, _ ->
               {:error, %{secret: "do-not-leak"}}
             end)

    refute message =~ "do-not-leak"
    identity = %{supervisor_pid: 123, worker_pid: 456, process_group: 456}

    assert {:error, [%{code: :semantic_worker_cleanup_unconfirmed, message: message}]} =
             Compiler.compile([model()], assets(), fn _, _ ->
               {:error, {:semantic_worker_cleanup_unconfirmed, identity}}
             end)

    assert message =~ "supervisor_pid=123"
    assert message =~ "worker_pid=456"
    assert message =~ "process_group=456"
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

  test "native input origins track repeated arguments, Unicode literals and inlined calls" do
    leaf = metric(:value, [:gross], "SUM(CASE WHEN 'ø' = 'ø' THEN @gross ELSE @gross END)")
    composed = metric(:doubled, [:gross], "value(@gross) + value(@gross)")
    compile!(model([composed, leaf]))

    for _ <- 1..2 do
      assert_receive {:validated, sql, [%{name: "gross", locations: locations}]}

      actual =
        Regex.scan(~r/"gross"/, sql, return: :index) |> Enum.map(fn [{index, _}] -> index end)

      assert locations == actual
    end

    refute_receive {:validated, _, _}
  end

  test "rejected relation syntax never invokes customer module metadata" do
    invalid = metric(:bad, [:gross], "SELECT SUM(@gross) FROM Favn.SemanticTest.AssetTrap")
    assert {:error, [_]} = Compiler.compile([model([invalid])], assets(), &validator/2)
    refute_receive :customer_asset_loaded

    for sql <- [
          "revenue((SELECT SUM(@gross) FROM Favn.SemanticTest.AssetTrap), @discount)",
          "revenue(revenue((SELECT SUM(@gross) FROM Favn.SemanticTest.AssetTrap), @discount), @discount)"
        ] do
      invalid = metric(:bad, [:gross, :discount], sql)

      assert {:error, [_]} =
               Compiler.compile([model([invalid, metric()])], assets(), &validator/2)

      refute_receive :customer_asset_loaded
    end
  end

  test "SQL syntax is ASCII while Unicode strings and comments remain valid" do
    for separator <- ["\u00A0", "\u200B", "\u2007", "\u202F"] do
      for sql <- ["SUM#{separator}(@gross)", "revenue(@gross, @discount) + sum#{separator}(1)"] do
        args = if String.starts_with?(sql, "SUM"), do: [:gross], else: [:gross, :discount]
        invalid = metric(:bad, args, sql)

        assert {:error, [%{code: :unsupported_sql_token}]} =
                 Compiler.compile([model([invalid, metric()])], assets(), &validator/2)
      end
    end

    valid =
      metric(
        :unicode,
        [:gross],
        "SUM(CASE WHEN 'ø\u00A0\u200B' = 'ø' THEN @gross ELSE 0 END) /* ø\u00A0\u200B */"
      )

    assert {:ok, _} = Compiler.compile([model([valid])], assets(), &validator/2)
  end

  test "relationship minimum grain inherits and compatibility validates referenced target keys" do
    [sales] = assets()

    store = %{
      ref: {Store, :asset},
      module: Store,
      type: :sql,
      depends_on: [],
      relation: RelationRef.new!(schema: "mart", name: "stores"),
      contract:
        Contract.new!(
          grain: [by: [:store_id]],
          columns: [%{name: :store_id, type: :integer, null: false}]
        )
    }

    relationship =
      Favn.SQL.Contract.Relationship.new!(
        name: :store,
        target: store.ref,
        on: [sale_id: :store_id],
        cardinality: :many_to_one,
        on_violation: :fail
      )

    sales = %{
      sales
      | depends_on: [store.ref],
        contract: %{sales.contract | relationships: [relationship]}
    }

    leaf = metric(:revenue, [:gross], "SUM(@gross)", minimum_grain: [:store])
    composed = metric(:doubled, [:gross], "revenue(@gross) * 2")
    artifact = compile!(model([composed, leaf]), [sales, store])

    assert Enum.find(hd(artifact.models)["metrics"], &(&1["name"] == "doubled"))["minimum_grain"] ==
             ["store"]

    assert %{status: :compatible} = Catalog.compatible(artifact, artifact.snapshot)

    {:ok, renamed} =
      Snapshot.build([sales, %{store | relation: %{store.relation | name: "other_stores"}}])

    assert %{status: :incompatible, reasons: [%{"reason" => "relationship_target_changed"}]} =
             Catalog.compatible(artifact, renamed)

    weakened = Map.update!(composed, :opts, &Keyword.put(&1, :minimum_grain, []))

    assert {:error, [%{code: :weakened_minimum_grain}]} =
             Compiler.compile([model([weakened, leaf])], [sales, store], &validator/2)
  end

  test "relationship compatibility preserves key order but ignores column declaration order" do
    [sales] = assets()

    store = %{
      ref: {Store, :asset},
      module: Store,
      type: :sql,
      depends_on: [],
      relation: RelationRef.new!(schema: "mart", name: "stores"),
      contract:
        Contract.new!(
          grain: [by: [:store_id, :region_id]],
          columns: [
            %{name: :region_id, type: :integer, null: false},
            %{name: :store_id, type: :integer, null: false}
          ]
        )
    }

    relationship =
      Favn.SQL.Contract.Relationship.new!(
        name: :store,
        target: store.ref,
        on: [sale_id: :store_id, units: :region_id],
        cardinality: :many_to_one,
        on_violation: :fail
      )

    sales = %{
      sales
      | depends_on: [store.ref],
        contract: %{sales.contract | relationships: [relationship]}
    }

    artifact = compile!(model(), [sales, store])

    reordered = %{
      store
      | contract: %{store.contract | columns: Enum.reverse(store.contract.columns)}
    }

    {:ok, served} = Snapshot.build([sales, reordered])
    assert %{status: :compatible} = Catalog.compatible(artifact, served)

    changed = %{
      reordered
      | contract: %{
          reordered.contract
          | grain: Favn.SQL.Contract.Grain.new!(by: [:region_id, :store_id])
        }
    }

    {:ok, served} = Snapshot.build([sales, changed])
    assert %{status: :incompatible} = Catalog.compatible(artifact, served)
  end

  test "column and grain descriptions produce informational contract diffs" do
    [sales] = assets()
    before = compile!(model(), [sales])

    columns =
      Enum.map(sales.contract.columns, fn column ->
        if column.name == :gross,
          do: %{column | description: "Gross amount before discounts"},
          else: column
      end)

    changed = %{
      sales
      | contract: %{
          sales.contract
          | columns: columns,
            grain: %{sales.contract.grain | description: "One row per sale"}
        }
    }

    after_artifact = compile!(model(), [changed])

    assert [
             %{
               "entity" => "contract:Sales.asset",
               "classification" => "informational",
               "changes" => changes
             }
           ] = Catalog.diff(before, after_artifact)

    assert changes == ["contract.columns.gross.description", "contract.grain_description"]

    columns =
      Enum.map(columns, fn column ->
        if column.name == :gross, do: %{column | type: :float}, else: column
      end)

    changed = %{changed | contract: %{changed.contract | columns: columns}}

    assert Enum.all?(
             Catalog.diff(before, compile!(model(), [changed])),
             &(&1["classification"] == "breaking")
           )
  end

  test "dependency depth and incremental expansion are bounded before native validation" do
    chain =
      Enum.reduce(1..33, [], fn index, acc ->
        name = String.to_atom("value_#{index}")
        sql = if index == 1, do: "SUM(@gross)", else: "value_#{index - 1}(@gross)"
        [metric(name, [:gross], sql) | acc]
      end)

    assert {:error, [%{code: :dependency_depth}]} =
             Compiler.compile([model(chain)], assets(), &validator/2)

    growth =
      Enum.reduce(1..16, [], fn index, acc ->
        name = String.to_atom("growth_#{index}")

        sql =
          if index == 1,
            do: "SUM(@gross)",
            else: "growth_#{index - 1}(@gross) + growth_#{index - 1}(@gross)"

        [metric(name, [:gross], sql) | acc]
      end)

    assert {:error, [%{code: :expansion_limit}]} =
             Compiler.compile([model(growth)], assets(), &validator/2)
  end

  test "diagnostics are bounded and report omitted count" do
    invalid = metric(:bad, [:missing], "SUM(@missing)")
    models = for index <- 1..105, do: %{model([invalid]) | name: String.to_atom("model_#{index}")}
    assert {:error, diagnostics} = Compiler.compile(models, assets(), &validator/2)
    assert length(diagnostics) == 100
    assert List.last(diagnostics).code == :diagnostics_omitted
    assert List.last(diagnostics).message =~ "6 additional"
    assert Enum.all?(diagnostics, &(byte_size(&1.message) <= 1024))
  end

  test "unknown JSON keys never create atoms and invalid units fail before hash acceptance" do
    {:ok, json} = compile!() |> Artifact.encode()
    wire = Jason.decode!(json)

    invalid =
      put_in(wire, ["models", Access.at(0), "metrics", Access.at(0), "unit", "value"], "nok")

    assert {:error, :invalid_semantic_artifact} = Artifact.decode(Jason.encode!(invalid))

    for index <- 1..100 do
      key = "unknown_semantic_field_#{index}_" <> Base.encode16(:crypto.strong_rand_bytes(8))
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end

      assert {:error, :invalid_semantic_artifact} =
               Artifact.decode(Jason.encode!(Map.put(wire, key, 1)))

      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end
  end

  @tag :tmp_dir
  test "oversized file reads are bounded and failed publication removes staging data", %{
    tmp_dir: directory
  } do
    large = Path.join(directory, "large.json")
    File.write!(large, String.duplicate("x", 16 * 1024 * 1024 + 1))
    assert {:error, :artifact_too_large} = Artifact.read(large)
    artifact = compile!()
    File.write!(Path.join(directory, artifact.semantic_version), "existing non-directory")
    assert {:error, _} = Artifact.write(artifact, directory)
    refute Enum.any?(File.ls!(directory), &String.starts_with?(&1, ".semantic-"))
  end
end

defmodule FavnDuckdbADBC.SemanticCompilerTest do
  use ExUnit.Case, async: false

  alias FavnDuckdbADBC.SemanticCompiler

  @moduletag :adbc_integration
  @inputs [
    %{name: "gross", type: :decimal, nullable: false},
    %{name: "discount", type: :decimal, nullable: false},
    %{name: "units", type: :integer, nullable: false},
    %{name: "category", type: :string, nullable: true}
  ]

  test "consumer macros preserve native join/filter plans and weighted ratios" do
    driver =
      Keyword.get(FavnDuckdbADBC.Runtime.driver_opts(), :driver) ||
        System.fetch_env!("DUCKDB_ADBC_DRIVER")

    assert {"ok\n", 0} =
             System.cmd("python3", [
               "-I",
               Path.join(__DIR__, "semantic_compiler_macro.py"),
               driver
             ])
  end

  test "validates aggregate expressions with explicit profile provenance" do
    assert {:ok, result} = SemanticCompiler.validate(~s|SUM("gross" - "discount")|, @inputs)
    assert result.native_type == "DECIMAL(38,2)"
    assert result.nullable == :unknown
    assert result.compiler_version == "duckdb-semantic-v1"
    assert result.validation_profile["gross"] == "DECIMAL(18,2)"
    assert result.runtime_version in ["v1.5.2", "v1.5.5"]
  end

  test "accepts SQL null handling, zero denominators, distinct strings, CASE, casts and FILTER" do
    for sql <- [
          ~s|SUM("gross" - "discount") / NULLIF(SUM("units"), 0)|,
          ~s|COUNT(DISTINCT "category")|,
          ~s|COALESCE(SUM("gross"), 0)|,
          ~s|CAST(SUM("gross") AS DECIMAL(38,10))|,
          ~s|SUM(CASE WHEN "units" > 0 AND "category" IS NOT NULL THEN "gross" ELSE 0 END)|,
          ~s|SUM("gross") FILTER (WHERE "units" > 0)|,
          ~s|ROUND(ABS(SUM("gross")), 2)|
        ] do
      assert {:ok, _} = SemanticCompiler.validate(sql, @inputs), sql
    end
  end

  test "native binding cannot establish business provenance for same-type arguments" do
    assert {:ok, _} = SemanticCompiler.validate(~s|SUM("discount" - "gross")|, @inputs)
  end

  test "compiler offsets distinguish generated inputs from otherwise-binding raw references" do
    inputs = [%{name: "gross", type: :decimal, nullable: false, locations: [4]}]
    assert {:ok, _} = SemanticCompiler.validate(~s|SUM("gross")|, inputs)

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|SUM("gross" + gross)|, inputs)

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|SUM("gross" + "gross")|, inputs)

    sql = ~s|SUM(CASE WHEN 'ø' = 'ø' THEN "gross" ELSE 0 END)|
    {offset, _length} = :binary.match(sql, ~s|"gross"|)
    assert {:ok, _} = SemanticCompiler.validate(sql, [%{hd(inputs) | locations: [offset]}])

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(sql, [%{hd(inputs) | locations: [offset - 2]}])
  end

  test "count accepts every supported logical input family" do
    types = [
      :integer,
      :float,
      :decimal,
      :string,
      :boolean,
      :date,
      :datetime,
      :binary,
      :time,
      :json,
      :uuid
    ]

    inputs = Enum.map(types, &%{name: Atom.to_string(&1), type: &1, nullable: true})
    sql = Enum.map_join(inputs, " + ", &~s|COUNT("#{&1.name}")|)
    assert {:ok, result} = SemanticCompiler.validate(sql, inputs)
    assert map_size(result.validation_profile) == length(types)
  end

  test "rejects independent reads and unsupported grammar before binding" do
    for sql <- [
          ~s|SUM("gross") + (SELECT 1)|,
          ~s|SUM("gross") FROM read_csv('/tmp/no-access')|,
          ~s|SUM("gross") OVER ()|,
          ~s|SUM("gross" ORDER BY "units")|,
          ~s|COUNT(*)|,
          ~s|SUM("missing")|,
          ~s|SUM(inputs."gross")|,
          ~s|SUM("gross") + random()|,
          ~s|SUM("gross") + current_setting('threads')|,
          ~s|SUM("gross"); SELECT 1|,
          ~s|SUM("gross"), SUM("discount")|,
          ~s|SUM(SUM("gross"))|,
          ~s|"gross" - "discount"|,
          ~s|SUM("gross") AS renamed|,
          ~s|SUM("gross") + $1|,
          ~s|SUM("gross") + unknown_helper(1)|
        ] do
      assert {:error, _} = SemanticCompiler.validate(sql, @inputs), sql
    end
  end

  test "binding rejects ungrouped columns and incompatible input types" do
    assert {:error, :semantic_bind_failed} =
             SemanticCompiler.validate(~s|SUM("gross") + "discount"|, @inputs)

    assert {:error, :semantic_bind_failed} =
             SemanticCompiler.validate(~s|SUM("category")|, @inputs)
  end

  test "bounds input before spawning and reports unavailable driver explicitly" do
    assert {:error, :invalid_semantic_input} =
             SemanticCompiler.validate(String.duplicate("x", 65_537), @inputs)

    config = Application.get_env(:favn, :duckdb_adbc)

    on_exit(fn ->
      if config,
        do: Application.put_env(:favn, :duckdb_adbc, config),
        else: Application.delete_env(:favn, :duckdb_adbc)
    end)

    Application.put_env(:favn, :duckdb_adbc, driver: "/missing/semantic-driver.so")
    assert {:error, :semantic_driver_unavailable} = SemanticCompiler.validate("SUM(1)", @inputs)
  end
end

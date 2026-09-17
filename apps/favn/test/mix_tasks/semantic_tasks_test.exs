defmodule Mix.Tasks.Favn.SemanticTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Semantic.{Artifact, Compiler}
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Column
  alias Mix.Tasks.Favn.Build.Semantics, as: BuildTask
  alias Mix.Tasks.Favn.ReadDoc, as: ReadDocTask
  alias Mix.Tasks.Favn.Semantic.Diff, as: DiffTask
  alias Mix.Tasks.Favn.Semantic.Inspect, as: InspectTask

  setup do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-tasks-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "inspect reads a finished artifact as JSON without customer source", %{
    directory: directory
  } do
    artifact = artifact("SUM(@units)")
    assert {:ok, %{path: path}} = Artifact.write(artifact, directory)

    output =
      capture_io(fn ->
        InspectTask.run(["--artifact", path, "--metric", "sales.units", "--format", "json"])
      end)

    assert {:ok, record} = Jason.decode(output)
    assert [%{"name" => "sales", "metrics" => [metric]}] = record["models"]
    assert metric["ref"] == "sales.units"

    assert [%{"position" => 1, "column" => "units", "contract_type" => "integer"}] =
             metric["inputs"]

    assert metric["validation"]["profile"] == %{"units" => "BIGINT"}
    assert metric["macro"]["evaluation"] == "aggregate_expression"
    assert metric["invocation"] =~ ~s|"sales_units"("source"."units")|
    refute output =~ "Elixir."
  end

  test "diff reports semantic formula changes between immutable local files", %{
    directory: directory
  } do
    before = artifact("SUM(@units)")
    after_artifact = artifact("SUM(@units) * 2")
    assert before.semantic_version != after_artifact.semantic_version
    assert before.snapshot_version == after_artifact.snapshot_version
    assert {:ok, %{path: before_path}} = Artifact.write(before, directory)
    assert {:ok, %{path: after_path}} = Artifact.write(after_artifact, directory)

    output =
      capture_io(fn ->
        DiffTask.run(["--from", before_path, "--to", after_path])
      end)

    assert {:ok, diff} = Jason.decode(output)
    assert is_list(diff)

    assert Enum.any?(
             diff,
             &(&1["entity"] == "metric:sales.units" and
                 &1["classification"] == "breaking")
           )
  end

  test "finished artifact tasks reject missing files and corrupt content", %{directory: directory} do
    missing = Path.join(directory, "missing.json")
    corrupt = Path.join(directory, "corrupt.json")
    File.write!(corrupt, "{invalid json")

    for path <- [missing, corrupt] do
      assert_raise Mix.Error, ~r/invalid semantic artifact/, fn ->
        InspectTask.run(["--artifact", path])
      end

      assert_raise Mix.Error, ~r/invalid semantic artifact/, fn ->
        DiffTask.run(["--from", path, "--to", path])
      end
    end
  end

  test "task arguments are explicit and reject unknown switches" do
    for {task, args, pattern} <- [
          {InspectTask, [], ~r/missing --artifact/},
          {InspectTask, ["--artifact", "unused", "--format", "xml"], ~r/format must be/},
          {InspectTask, ["--artifact", "unused", "--unknown"], ~r/invalid option/},
          {DiffTask, [], ~r/missing --from or --to/},
          {DiffTask, ["--from", "unused"], ~r/missing --from or --to/},
          {BuildTask, [], ~r/missing --output/},
          {BuildTask, ["--output", "unused", "--input", "old-mode"], ~r/invalid option/}
        ] do
      assert_raise Mix.Error, pattern, fn -> task.run(args) end
    end
  end

  test "build rejects absent or default build paths" do
    previous = System.get_env("MIX_BUILD_PATH")

    on_exit(fn ->
      if previous,
        do: System.put_env("MIX_BUILD_PATH", previous),
        else: System.delete_env("MIX_BUILD_PATH")
    end)

    System.delete_env("MIX_BUILD_PATH")

    assert_raise Mix.Error, ~r/semantic_build_path_required/, fn ->
      BuildTask.run(["--output", "unused"])
    end

    for path <- ["_build", "_build/test"] do
      System.put_env("MIX_BUILD_PATH", path)

      assert_raise Mix.Error, ~r/semantic_build_path_shared/, fn ->
        BuildTask.run(["--output", "unused"])
      end
    end
  end

  test "public AI and SQLAsset docs route authors to the semantic DSL" do
    ai = capture_io(fn -> ReadDocTask.run(["Favn.AI"]) end)
    assert ai =~ "mix favn.read_doc Favn.SQLAsset semantic"
    assert ai =~ "sql-semantic-models.html"
    assert ai =~ "ordered bindings"

    sql_asset = capture_io(fn -> ReadDocTask.run(["Favn.SQLAsset", "semantic"]) end)
    assert sql_asset =~ "semantic/2"
    assert sql_asset =~ "metric revenue(gross_value, discount_value)"
    assert sql_asset =~ "after `contract` and before `query`"
  end

  defp artifact(sql) do
    contract = Contract.new!(columns: [Column.new!(:units, :integer, null: false)])

    asset = %{
      ref: {__MODULE__, :sales},
      module: __MODULE__,
      type: :sql,
      depends_on: [],
      relation: %{connection: :analytics, schema: "mart", name: "fct_sales"},
      contract: contract
    }

    metric = %{
      name: :units,
      args: [:units],
      sql: sql,
      file: "source.ex",
      line: 1,
      opts: [unit: :count, time_aggregate: :aggregate, description: "Units sold"]
    }

    model = %{
      name: :sales,
      module: __MODULE__,
      time: nil,
      dimension: nil,
      hierarchies: [],
      metrics: [metric],
      file: "source.ex",
      line: 1
    }

    validator = fn _sql, _inputs ->
      {:ok,
       %{
         native_type: "HUGEINT",
         nullable: :unknown,
         runtime_version: "test",
         compiler_version: "test",
         validation_profile: %{"units" => "BIGINT"}
       }}
    end

    assert {:ok, artifact} = Compiler.compile([model], [asset], validator)
    artifact
  end
end

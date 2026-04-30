defmodule FavnOrchestrator.Storage.PayloadCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.Pipeline.Definition, as: PipelineDefinition
  alias Favn.RelationRef
  alias Favn.SQL.Template
  alias Favn.Triggers.Schedule
  alias Favn.Window.Policy
  alias FavnOrchestrator.Storage.PayloadCodec

  test "round-trips tagged runtime payload values" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    payload = %{
      asset_ref: {MyApp.Asset, :asset},
      status: :running,
      happened_at: now,
      nested: [%{reason: {:cancelled, :operator}}],
      scheduler: %Favn.Scheduler.State{
        pipeline_module: MyApp.Pipeline,
        schedule_id: :daily,
        version: 2,
        last_due_at: now
      }
    }

    assert {:ok, encoded} = PayloadCodec.encode(payload)
    assert encoded =~ "json-v1"
    assert encoded =~ "Elixir.MyApp.Asset"

    assert {:ok, decoded} = PayloadCodec.decode(encoded)
    assert decoded == payload
  end

  test "round-trips manifest versions with SQL asset templates" do
    asset_ref = {MyApp.SQLAssets.DailyOrders, :asset}

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/fixtures/payload_codec.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    manifest = %Manifest{
      assets: [
        %Asset{
          ref: asset_ref,
          module: elem(asset_ref, 0),
          name: :asset,
          type: :sql,
          relation: RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "orders"}),
          sql_execution: %SQLExecution{
            sql: "SELECT 1 AS id",
            template: template,
            sql_definitions: []
          }
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.SQLDailyOrders,
          name: :daily_orders,
          selectors: [{:asset, asset_ref}],
          deps: :all,
          source: :dsl,
          outputs: [:asset]
        }
      ],
      graph: %Graph{nodes: [asset_ref], edges: [], topo_order: [asset_ref]}
    }

    assert {:ok, version} = Version.new(manifest, manifest_version_id: "mv_sql_payload_codec")
    assert {:ok, encoded} = PayloadCodec.encode(version)
    assert {:ok, decoded} = PayloadCodec.decode(encoded)

    assert %Version{} = decoded
    assert %SQLExecution{template: %Template{}} = hd(decoded.manifest.assets).sql_execution
    assert decoded == version
  end

  test "round-trips pipeline definitions with inline trigger schedules" do
    assert {:ok, schedule} =
             Schedule.new_inline(
               cron: "0 * * * *",
               timezone: "Etc/UTC",
               missed: :skip,
               overlap: :forbid
             )

    definition = %PipelineDefinition{
      module: MyApp.Pipelines.Scheduled,
      name: :scheduled,
      selectors: [{:asset, {MyApp.Assets.Scheduled, :asset}}],
      schedule: {:inline, schedule},
      window: Policy.new!(:day),
      source: :dsl,
      outputs: [:asset]
    }

    assert {:ok, encoded} = PayloadCodec.encode(definition)
    assert {:ok, decoded} = PayloadCodec.decode(encoded)

    assert %PipelineDefinition{schedule: {:inline, %Schedule{}}} = decoded
    assert decoded == definition
  end

  test "rejects unknown atoms during decode" do
    payload =
      ~s({"format":"json-v1","value":{"__type__":"atom","value":"favn_unknown_payload_atom"}})

    assert {:error, {:payload_decode_failed, {:unknown_atom, "favn_unknown_payload_atom"}}} =
             PayloadCodec.decode(payload)
  end

  test "rejects unsupported struct modules during decode" do
    payload =
      ~s({"format":"json-v1","value":{"__type__":"struct","module":"Elixir.URI","fields":{"__type__":"map","entries":[]}}})

    assert {:error, {:payload_decode_failed, {:unsupported_struct_module, "Elixir.URI"}}} =
             PayloadCodec.decode(payload)
  end
end

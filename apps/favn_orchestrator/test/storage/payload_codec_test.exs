defmodule FavnOrchestrator.Storage.PayloadCodecTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Pipeline.Definition, as: PipelineDefinition
  alias Favn.Plan.NodeIdentity
  alias Favn.RelationRef
  alias Favn.Triggers.Schedule
  alias Favn.Window.Policy
  alias FavnOrchestrator.RunState
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

  test "round-trips every supported temporal and decimal runtime input value" do
    oslo_datetime =
      DateTime.new!(
        ~D[2026-07-01],
        ~T[12:34:56.123456],
        "Europe/Oslo",
        Favn.Timezone.database!()
      )
      |> Map.put(:zone_abbr, "LEGACY_SUMMER_TIME")

    values = [
      ~D[2026-07-01],
      ~T[12:34:56.123456],
      ~N[2026-07-01 12:34:56.123456],
      oslo_datetime,
      Decimal.new(-1, 12_340, -3)
    ]

    encoded_values =
      Enum.map(values, fn value ->
        assert {:ok, encoded} = PayloadCodec.encode(%{value: value})
        {value, encoded}
      end)

    without_timezone_database(fn ->
      for {value, encoded} <- encoded_values do
        assert {:ok, %{value: ^value}} = PayloadCodec.decode(encoded)
      end
    end)
  end

  test "round-trips compact manifest versions with package hashes" do
    asset_ref = {MyApp.SQLAssets.DailyOrders, :asset}

    manifest = %Manifest{
      assets: [
        %Asset{
          ref: asset_ref,
          module: elem(asset_ref, 0),
          name: :asset,
          type: :sql,
          relation: RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "orders"}),
          execution_package_hash: String.duplicate("b", 64)
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
    assert hd(decoded.manifest.assets).execution_package_hash == String.duplicate("b", 64)
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

  test "round-trips runner results with explicit runner error and asset result contracts" do
    started_at = DateTime.utc_now() |> DateTime.truncate(:second)
    finished_at = DateTime.add(started_at, 25, :millisecond)

    error =
      RunnerError.normalize(%{
        type: :missing_runtime_config,
        phase: :asset_runtime_config,
        message: "missing required asset runtime config",
        details: %{provider: :env, token: "secret-token", asset_retryable?: false}
      })

    result = %RunnerResult{
      run_id: "run_payload_runner_result",
      manifest_version_id: "mv_payload_runner_result",
      manifest_content_hash: String.duplicate("b", 64),
      status: :error,
      error: error,
      asset_results: [
        %RunnerAssetResult{
          ref: {MyApp.Asset, :asset},
          status: :error,
          started_at: started_at,
          finished_at: finished_at,
          duration_ms: 25,
          error: error,
          attempt_count: 2,
          max_attempts: 3,
          attempts: [%{attempt: 2, status: :error, error: error, meta: %{}}]
        }
      ]
    }

    assert {:ok, encoded} = PayloadCodec.encode(result)
    assert {:ok, decoded} = PayloadCodec.decode(encoded)

    assert decoded == result
    assert decoded.error.details.token == :redacted
  end

  test "round-trips runner work with explicit node identity" do
    identity = %NodeIdentity{
      manifest_version_id: "mv_payload_runner_work",
      node_key: {{MyApp.Asset, :asset}, nil},
      target_refs: [{MyApp.Asset, :asset}],
      planned_asset_refs: [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}],
      execution_pool: :default
    }

    work = %RunnerWork{
      run_id: "run_payload_runner_work",
      run_started_at: ~U[2026-07-17 08:30:00Z],
      manifest_version_id: "mv_payload_runner_work",
      manifest_content_hash: String.duplicate("c", 64),
      node_identity: identity,
      asset_ref: {MyApp.Asset, :asset},
      asset_refs: [{MyApp.Asset, :asset}],
      planned_asset_refs: identity.planned_asset_refs,
      attempt: 2,
      max_attempts: 3,
      asset_step_id: "step_payload_runner_work",
      stage: 1
    }

    assert {:ok, encoded} = PayloadCodec.encode(work)
    assert {:ok, decoded} = PayloadCodec.decode(encoded)

    assert decoded == work
  end

  test "rejects unknown atoms during decode" do
    payload =
      ~s({"format":"json-v1","value":{"__type__":"atom","value":"favn_unknown_payload_atom"}})

    assert {:error, {:payload_decode_failed, {:unknown_atom, "favn_unknown_payload_atom"}}} =
             PayloadCodec.decode(payload)
  end

  test "rejects consumer module atoms that are not already loaded" do
    unknown_module = "Elixir.Favn.PayloadCodecRestartFixture.Asset"
    existing_module = __MODULE__.ExistingAsset

    run =
      RunState.new(
        id: "run_payload_restart",
        manifest_version_id: "mv_payload_restart",
        manifest_content_hash: String.duplicate("a", 64),
        asset_ref: {existing_module, :asset},
        target_refs: [{existing_module, :asset}]
      )

    assert {:ok, encoded} = PayloadCodec.encode(run)

    encoded = replace_atom_value(encoded, Atom.to_string(existing_module), unknown_module)

    assert {:error, {:payload_decode_failed, {:unknown_atom, ^unknown_module}}} =
             PayloadCodec.decode(encoded)
  end

  test "rejects unsupported struct modules during decode" do
    payload =
      ~s({"format":"json-v1","value":{"__type__":"struct","module":"Elixir.URI","fields":{"__type__":"map","entries":[]}}})

    assert {:error, {:payload_decode_failed, {:unsupported_struct_module, "Elixir.URI"}}} =
             PayloadCodec.decode(payload)
  end

  defp replace_atom_value(encoded, from, to) do
    encoded
    |> JSON.decode!()
    |> replace_atom_value_in_term(from, to)
    |> JSON.encode!()
  end

  defp replace_atom_value_in_term(%{"__type__" => "atom", "value" => value} = term, value, to) do
    %{term | "value" => to}
  end

  defp replace_atom_value_in_term(%{} = term, from, to) do
    Map.new(term, fn {key, value} -> {key, replace_atom_value_in_term(value, from, to)} end)
  end

  defp replace_atom_value_in_term(values, from, to) when is_list(values) do
    Enum.map(values, &replace_atom_value_in_term(&1, from, to))
  end

  defp replace_atom_value_in_term(value, _from, _to), do: value

  defp without_timezone_database(function) do
    previous = Application.fetch_env(:favn_core, :time_zone_database)
    Application.put_env(:favn_core, :time_zone_database, __MODULE__.UnavailableTimezoneDatabase)

    try do
      function.()
    after
      case previous do
        {:ok, database} -> Application.put_env(:favn_core, :time_zone_database, database)
        :error -> Application.delete_env(:favn_core, :time_zone_database)
      end
    end
  end
end

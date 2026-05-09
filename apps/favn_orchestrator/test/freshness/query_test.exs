defmodule FavnOrchestrator.Freshness.QueryTest do
  use ExUnit.Case, async: false

  alias Favn.Freshness.Key
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @now ~U[2026-05-09 12:00:00Z]
  @raw_ref {__MODULE__.Raw, :asset}
  @stage_ref {__MODULE__.Stage, :asset}
  @raw_node_key {@raw_ref, nil}
  @stage_node_key {@stage_ref, nil}

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "gets and lists stored freshness states through orchestrator facade" do
    raw_state = freshness_state(@raw_ref, @raw_node_key, Key.latest(), version: "raw:v1")
    stage_state = freshness_state(@stage_ref, @stage_node_key, Key.latest(), version: "stage:v1")

    assert :ok = Storage.put_asset_freshness_state(raw_state)
    assert :ok = Storage.put_asset_freshness_state(stage_state)

    assert {:ok, ^raw_state} = FavnOrchestrator.get_asset_freshness(@raw_ref, Key.latest())

    assert {:ok, page} =
             FavnOrchestrator.list_asset_freshness(asset_ref_module: elem(@stage_ref, 0))

    assert [^stage_state] = page.items
  end

  test "explains fresh asset state from unchanged upstream versions" do
    raw_state = freshness_state(@raw_ref, @raw_node_key, Key.latest(), version: "raw:v1")

    stage_state =
      freshness_state(@stage_ref, @stage_node_key, Key.latest(),
        version: "stage:v1",
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_node_key,
            freshness_version: "raw:v1",
            success_run_id: "run_raw"
          }
        ]
      )

    assert :ok = Storage.put_asset_freshness_state(raw_state)
    assert :ok = Storage.put_asset_freshness_state(stage_state)

    assert {:ok,
            %{
              asset_ref: @stage_ref,
              freshness_key: "latest",
              latest_success_run_id: "run_asset",
              status: :fresh,
              stale_reasons: []
            }} =
             FavnOrchestrator.explain_asset_staleness(@stage_ref,
               upstream_node_keys: [@raw_node_key]
             )
  end

  test "explains stale asset state from changed upstream versions" do
    raw_state = freshness_state(@raw_ref, @raw_node_key, Key.latest(), version: "raw:v2")

    stage_state =
      freshness_state(@stage_ref, @stage_node_key, Key.latest(),
        version: "stage:v1",
        input_versions: [
          %{
            upstream_ref: @raw_ref,
            upstream_node_key: @raw_node_key,
            freshness_version: "raw:v1",
            success_run_id: "run_raw_old"
          }
        ]
      )

    assert :ok = Storage.put_asset_freshness_state(raw_state)
    assert :ok = Storage.put_asset_freshness_state(stage_state)

    assert {:ok,
            %{
              asset_ref: @stage_ref,
              status: :stale,
              stale_reasons: [
                %{
                  type: :upstream_version_changed,
                  upstream_ref: @raw_ref,
                  upstream_node_key: @raw_node_key,
                  consumed_version: "raw:v1",
                  current_version: "raw:v2",
                  current_success_run_id: "run_asset"
                }
              ]
            }} =
             FavnOrchestrator.explain_asset_staleness(@stage_ref,
               upstream_node_keys: [@raw_node_key]
             )
  end

  defp freshness_state(ref, node_key, freshness_key, opts) do
    {module, name} = ref
    run_id = Keyword.get(opts, :run_id, "run_#{name}")

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: freshness_key,
        status: Keyword.get(opts, :status, :ok),
        freshness_version: Keyword.fetch!(opts, :version),
        latest_success_run_id: run_id,
        latest_success_node_key: node_key,
        latest_success_at: @now,
        latest_attempt_run_id: run_id,
        latest_attempt_status: Keyword.get(opts, :status, :ok),
        latest_attempt_at: @now,
        input_versions: Keyword.get(opts, :input_versions, []),
        updated_at: @now
      })

    state
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end

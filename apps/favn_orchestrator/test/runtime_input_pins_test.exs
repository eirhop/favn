defmodule FavnOrchestrator.RuntimeInputPinsTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Plan.NodeIdentity
  alias Favn.RuntimeInput.Resolution
  alias Favn.RuntimeInputResolver.Ref, as: ResolverRef
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RuntimeInputPins
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @asset_ref {MyApp.RuntimeInputAsset, :asset}
  @node_key {@asset_ref, nil}

  defmodule RunnerClientStub do
    @moduledoc false

    def resolve_runtime_inputs(_work, opts) do
      counter = Keyword.fetch!(opts, :counter)

      sequence =
        Agent.get_and_update(counter, fn sequence ->
          next = sequence + 1
          {next, next}
        end)

      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: sequence, token: "secret-#{sequence}"},
        input_identity: "input-#{sequence}",
        metadata: %{source: "test"},
        sensitive_params: [:token]
      )
    end
  end

  setup do
    Memory.reset()

    start_supervised!(%{
      id: __MODULE__.Counter,
      start: {Agent, :start_link, [fn -> 0 end, [name: __MODULE__.Counter]]}
    })

    on_exit(fn -> Memory.reset() end)

    :ok
  end

  test "fresh resolution is pinned once and reused without exposing parameters" do
    work = work("fresh-run", :fresh)

    assert {:ok, first} = prepare(work)
    assert first.runtime_input_pin.params.account_id == 1
    assert first.runtime_input_pin.input_identity == "input-1"
    assert first.metadata.runtime_input_event == :runtime_inputs_resolved
    refute inspect(first.runtime_input_pin) =~ "secret-1"
    refute inspect(first.metadata) =~ "secret-1"

    assert {:ok, second} = prepare(work)
    assert second.runtime_input_pin == first.runtime_input_pin
    assert second.metadata.runtime_input_event == :runtime_inputs_pin_reused
    assert Agent.get(__MODULE__.Counter, & &1) == 1

    assert second.metadata.runtime_input_lineage == %{
             node_key: @node_key,
             resolver: MyApp.RuntimeInputResolver,
             input_identity: "input-1",
             payload_fingerprint: first.runtime_input_pin.payload_fingerprint,
             source_run_id: nil,
             source_node_key: nil,
             source_payload_fingerprint: nil
           }
  end

  test "public run details expose lineage without resolved parameters" do
    run_id = "public-lineage-run"

    assert :ok =
             Storage.put_run(
               RunState.new(
                 id: run_id,
                 manifest_version_id: "mv-runtime-inputs",
                 manifest_content_hash: String.duplicate("a", 64),
                 asset_ref: @asset_ref
               )
             )

    assert {:ok, _prepared} = prepare(work(run_id, :fresh))
    assert {:ok, run} = FavnOrchestrator.get_run(run_id)

    assert [lineage] = run.metadata.runtime_input_lineage
    assert lineage.node_key == @node_key
    assert lineage.input_identity == "input-1"
    refute Map.has_key?(lineage, :params)
    refute inspect(run.metadata) =~ "secret-1"
  end

  test "pinned replay copies the source pin and records lineage" do
    assert {:ok, source} = prepare(work("source-run", :fresh))

    replay =
      "replay-run"
      |> work(:pinned)
      |> put_in([Access.key!(:metadata), :source_run_id], "source-run")

    assert {:ok, prepared} = prepare(replay)
    pin = prepared.runtime_input_pin

    assert pin.params == source.runtime_input_pin.params
    assert pin.payload_fingerprint == source.runtime_input_pin.payload_fingerprint
    assert pin.source_run_id == "source-run"
    assert pin.source_node_key == @node_key
    assert pin.source_payload_fingerprint == source.runtime_input_pin.payload_fingerprint
    assert prepared.metadata.runtime_input_event == :runtime_inputs_pin_inherited
    assert Agent.get(__MODULE__.Counter, & &1) == 1
  end

  test "inherit resolves fresh only when the source pin is missing" do
    replay =
      "inherit-run"
      |> work(:inherit)
      |> put_in([Access.key!(:metadata), :source_run_id], "missing-run")

    assert {:ok, prepared} = prepare(replay)
    assert prepared.runtime_input_pin.input_identity == "input-1"
    assert prepared.runtime_input_pin.source_run_id == nil
    assert Agent.get(__MODULE__.Counter, & &1) == 1
  end

  test "pinned replay fails closed when the source pin is missing" do
    replay =
      "pinned-run"
      |> work(:pinned)
      |> put_in([Access.key!(:metadata), :source_run_id], "missing-run")

    assert {:error, {:pinned, :runtime_input_pin_not_found}} = prepare(replay)

    assert {:error, :runtime_input_pin_not_found} =
             Storage.get_runtime_input_pin("pinned-run", @node_key)

    assert Agent.get(__MODULE__.Counter, & &1) == 0
  end

  test "invalid replay input modes fail instead of silently resolving fresh" do
    assert {:error, :invalid_input_mode} = prepare(work("invalid-run", :surprise))

    assert Agent.get(__MODULE__.Counter, & &1) == 0
  end

  defp prepare(work) do
    RuntimeInputPins.prepare(work, manifest_version(), RunnerClientStub,
      counter: __MODULE__.Counter
    )
  end

  defp work(run_id, mode) do
    %RunnerWork{
      run_id: run_id,
      manifest_version_id: "mv-runtime-inputs",
      manifest_content_hash: String.duplicate("a", 64),
      node_identity: %NodeIdentity{
        manifest_version_id: "mv-runtime-inputs",
        node_key: @node_key,
        target_refs: [@asset_ref],
        planned_asset_refs: [@asset_ref]
      },
      asset_ref: @asset_ref,
      asset_refs: [@asset_ref],
      planned_asset_refs: [@asset_ref],
      metadata: %{runtime_input_mode: mode}
    }
  end

  defp manifest_version do
    %Version{
      manifest_version_id: "mv-runtime-inputs",
      content_hash: String.duplicate("a", 64),
      schema_version: 7,
      runner_contract_version: 7,
      manifest: %Manifest{
        assets: [
          %Asset{
            ref: @asset_ref,
            type: :sql,
            sql_execution: %{runtime_inputs: %ResolverRef{module: MyApp.RuntimeInputResolver}}
          }
        ]
      }
    }
  end
end

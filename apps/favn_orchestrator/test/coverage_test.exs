defmodule FavnOrchestrator.CoverageTest do
  use ExUnit.Case, async: false

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.Coverage
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @evaluated_at ~U[2026-07-10 12:00:00Z]
  @asset_ref {__MODULE__.Asset, :orders}

  defmodule FakeStore do
    def get_runtime_state(_query), do: {:ok, Process.get(:coverage_runtime)}
    def get_deployment_targets(_query), do: {:ok, Process.get(:coverage_targets)}
    def get_deployment_manifest(_query), do: {:ok, Process.get(:coverage_version)}

    def count_successful_asset_windows(_query) do
      case Process.get(:coverage_count_result, {:ok, 0}) do
        {:ok, count} -> {:ok, count}
        {:error, reason} -> {:error, reason}
      end
    end

    def get_successful_asset_window_keys(query) do
      case Process.get(:coverage_keys_result, :ok) do
        :ok ->
          successful = MapSet.new(Process.get(:coverage_successful_keys, []))
          {:ok, Enum.filter(query.window_keys, &MapSet.member?(successful, &1))}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  setup do
    target_id = ManifestTarget.asset_id(@asset_ref)
    version = version("semantic-a", coverage())

    Process.put(:coverage_version, version)

    Process.put(:coverage_runtime, %RuntimeState{
      workspace_id: "coverage-workspace",
      deployment_id: "coverage-deployment",
      manifest_version_id: version.manifest_version_id,
      revision: 1
    })

    Process.put(:coverage_targets, [
      %DeploymentTarget{
        target_kind: :asset,
        target_id: target_id,
        selection_source: :common,
        customer_visible: true,
        descriptor: %{}
      }
    ])

    Process.put(:coverage_count_result, {:ok, 0})
    Process.put(:coverage_keys_result, :ok)
    Process.put(:coverage_successful_keys, [])

    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      rebuilds: FakeStore,
      target_operation_locks: FakeStore,
      materialization: FakeStore,
      backfills: FakeStore,
      operator_reads: FakeStore,
      logs: FakeStore,
      identity: FakeStore,
      maintenance: FakeStore
    }

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    assert {:ok, pid} = PersistenceRuntime.start_link(runtime)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    {:ok, context} =
      WorkspaceContext.new("coverage-workspace", "coverage-test", [:customer_operator])

    {:ok, context: context, target_id: target_id}
  end

  test "reports generation-aware counts and pages missing windows", fixture do
    assert {:ok, summary} =
             Coverage.summary(fixture.context, fixture.target_id, evaluated_at: @evaluated_at)

    assert summary.status == :incomplete
    assert summary.expected_count == 3
    assert summary.covered_count == 0
    assert summary.missing_count == 3
    assert summary.evidence_generation_id == "semantic-a"

    assert {:ok, first} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 2
             )

    assert Enum.map(first.items, & &1.start_at.day) == [1, 2]
    assert first.pagination.has_more
    assert is_binary(first.pagination.next_cursor)

    assert {:ok, second} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               cursor: first.pagination.next_cursor,
               limit: 2
             )

    assert Enum.map(second.items, & &1.start_at.day) == [3]
    refute second.pagination.has_more
  end

  test "rejects a cursor after the evidence generation changes", fixture do
    assert {:ok, page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 1
             )

    Process.put(:coverage_version, version("semantic-b", coverage()))

    assert {:error, :coverage_cursor_stale} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               cursor: page.pagination.next_cursor,
               limit: 1
             )
  end

  test "returns explicit unknown states", fixture do
    Process.put(:coverage_version, version("semantic-a", nil))

    assert {:ok, undeclared} =
             Coverage.summary(fixture.context, fixture.target_id, evaluated_at: @evaluated_at)

    assert undeclared.status == :unknown
    assert undeclared.unknown_reason == :coverage_not_declared

    Process.put(:coverage_version, version("semantic-a", coverage()))
    Process.put(:coverage_count_result, {:error, :unavailable})

    assert {:ok, unavailable} =
             Coverage.summary(fixture.context, fixture.target_id, evaluated_at: @evaluated_at)

    assert unavailable.status == :unknown
    assert unavailable.unknown_reason == :authoritative_state_unavailable
  end

  test "returns unknown when exact successful keys cannot be read", fixture do
    Process.put(:coverage_keys_result, {:error, :unavailable})

    assert {:ok, page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    assert page.summary.status == :unknown
    assert page.summary.unknown_reason == :authoritative_state_unavailable
    assert page.items == []
    refute page.pagination.has_more
  end

  test "keeps authoritative read failures explicit on later pages", fixture do
    assert {:ok, first_page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 1
             )

    Process.put(:coverage_keys_result, {:error, :unavailable})

    assert {:ok, unavailable_page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               cursor: first_page.pagination.next_cursor,
               limit: 1
             )

    assert unavailable_page.summary.status == :unknown
    assert unavailable_page.summary.unknown_reason == :authoritative_state_unavailable
    assert unavailable_page.items == []
  end

  test "freezes exact missing keys and rejects a changed selection", fixture do
    assert {:ok, plan} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    assert plan.window_count == 3
    assert length(plan.windows) == 3
    assert is_binary(plan.plan_id)
    assert is_binary(plan.plan_hash)

    [first | _rest] = plan.windows
    Process.put(:coverage_count_result, {:ok, 1})
    Process.put(:coverage_successful_keys, ["window:" <> first.window_key])

    assert {:error, :coverage_selection_stale} =
             Coverage.submit_missing_backfill(fixture.context, fixture.target_id, plan)
  end

  test "can freeze one bounded page instead of the full missing set", fixture do
    assert {:ok, plan} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 2
             )

    assert plan.selection == %{mode: :page, cursor: nil, limit: 2}
    assert plan.window_count == 2
    assert Enum.map(plan.windows, & &1.start_at.day) == [1, 2]
  end

  test "emits bounded coverage query telemetry", fixture do
    handler = "coverage-query-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        [:favn, :orchestrator, :coverage_query],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, _summary} =
             Coverage.summary(fixture.context, fixture.target_id, evaluated_at: @evaluated_at)

    assert_receive {[:favn, :orchestrator, :coverage_query],
                    %{duration: duration, result_count: 1},
                    %{operation: :summary, status: :incomplete}}

    assert is_integer(duration) and duration >= 0
  end

  defp coverage do
    window = WindowSpec.new!(:day, timezone: "Etc/UTC")

    {:ok, coverage} =
      Effective.resolve(
        Spec.new!(from: ~D[2026-07-01], through: ~D[2026-07-03]),
        window,
        nil
      )

    coverage
  end

  defp version(semantic_generation_id, coverage) do
    asset = %Asset{
      ref: @asset_ref,
      module: elem(@asset_ref, 0),
      name: elem(@asset_ref, 1),
      type: :source,
      window: WindowSpec.new!(:day, timezone: "Etc/UTC"),
      coverage: coverage,
      semantic_generation_id: semantic_generation_id
    }

    %Version{
      manifest_version_id: "coverage-manifest",
      content_hash: "sha256:coverage-manifest",
      schema_version: 11,
      runner_contract_version: 11,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      manifest: %Manifest{
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        assets: [asset]
      }
    }
  end
end

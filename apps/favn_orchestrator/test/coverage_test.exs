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
  alias FavnOrchestrator.Persistence.Results.EvidenceBinding
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
    def get_deployment_configuration(_query), do: {:ok, %{}}

    def get_evidence_bindings(query) do
      generation_id =
        Process.get(:coverage_evidence_generation) ||
          raise "coverage evidence generation was not configured"

      {:ok,
       Enum.map(query.target_ids, fn target_id ->
         %EvidenceBinding{
           workspace_id: query.workspace_context.workspace_id,
           target_id: target_id,
           evidence_generation_id: generation_id,
           initial_manifest_id: "coverage-manifest",
           created_at: ~U[2026-07-01 00:00:00Z]
         }
       end)}
    end

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
    Process.put(:coverage_evidence_generation, "semantic-a")

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
      run_submissions: FakeStore,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      target_recovery: FakeStore,
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
    start_supervised!({PersistenceRuntime, runtime})

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

  test "reports covered windows as well as missing ones, and the range's own bounds",
       fixture do
    assert {:ok, bare} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    # The evidence key comes from the window the query itself reported, so the test
    # cannot pass by agreeing with a hand-written encoding that has drifted.
    [_first, second, _third] = bare.windows
    Process.put(:coverage_count_result, {:ok, 1})
    Process.put(:coverage_successful_keys, ["window:" <> second.window_key])

    assert {:ok, states} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    assert states.kind == :day
    assert states.timezone == "Etc/UTC"

    # A calendar cannot work the complement out for itself, so both halves are named.
    # Nothing here says the second day is covered except the evidence for it.
    assert Enum.map(states.windows, &{&1.start_at.day, &1.covered?}) ==
             [{1, false}, {2, true}, {3, false}]

    # The bounds are the whole range, not this page, because they exist to say how far
    # back navigation may go.
    assert states.first_expected_at.day == 1
    assert states.last_expected_at.day == 3
    assert states.summary.missing_count == 2
  end

  test "addresses a range by local dates, exclusive at the top and clamped to coverage",
       fixture do
    assert {:ok, middle} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               from: ~D[2026-07-02],
               until: ~D[2026-07-03]
             )

    # The upper bound is exclusive, so this is the second day and only the second day.
    # A caller walking unit by unit never sees a period twice.
    assert Enum.map(middle.windows, & &1.start_at.day) == [2]

    assert {:ok, before} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               from: ~D[2020-01-01],
               until: ~D[2026-07-02]
             )

    # Before coverage begins is the beginning, so stepping back past the start lands on
    # the start rather than on nothing.
    assert Enum.map(before.windows, & &1.start_at.day) == [1]

    assert {:ok, past} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               from: ~D[2030-01-01]
             )

    # Past the end is empty rather than clamped back to the last period: a caller
    # stepping forward has to be able to tell that it ran out.
    assert past.windows == []
    assert past.last_expected_at.day == 3
  end

  # The screen is one calendar unit, and its length is not a number the caller can
  # know: February holds 28 days, a clock change makes a day hold 23 or 25 hours.
  # Asking by count returned periods from the next unit, which the calendar drew under
  # this unit's heading and offered for backfill.
  test "a range never reaches into the unit after it", fixture do
    Process.put(
      :coverage_version,
      version("semantic-a", coverage(~D[2026-07-01], ~D[2026-08-31]))
    )

    assert {:ok, july} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: ~U[2026-09-15 00:00:00Z],
               from: ~D[2026-07-01],
               until: ~D[2026-08-01]
             )

    assert length(july.windows) == 31
    assert Enum.all?(july.windows, &(&1.start_at.month == 7))
  end

  test "rejects a range bound that is not a date", fixture do
    assert {:error, :invalid_coverage_options} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               from: ~U[2026-07-02 00:00:00Z]
             )
  end

  test "coverage that cannot be evaluated has no windows and no range", fixture do
    Process.put(:coverage_version, version("semantic-a", nil))

    assert {:ok, states} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    assert states.summary.status == :unknown
    assert states.windows == []
    assert states.kind == nil
    assert states.first_expected_at == nil
  end

  test "an unreadable evidence generation reports no windows rather than empty ones",
       fixture do
    Process.put(:coverage_keys_result, {:error, :unavailable})

    assert {:ok, states} =
             Coverage.window_states(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    # Returning three uncovered windows here would draw a calendar of gaps that may
    # not exist, which is worse than drawing nothing.
    assert states.windows == []
    assert states.summary.status == :unknown
  end

  test "rejects a cursor after the durable evidence binding changes", fixture do
    assert {:ok, page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 1
             )

    Process.put(:coverage_evidence_generation, "semantic-b")

    assert {:error, :coverage_cursor_stale} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               cursor: page.pagination.next_cursor,
               limit: 1
             )
  end

  test "retains a cursor when only the manifest semantic generation changes", fixture do
    assert {:ok, page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               limit: 1
             )

    Process.put(:coverage_version, version("semantic-b", coverage()))

    assert {:ok, next_page} =
             Coverage.missing_windows(fixture.context, fixture.target_id,
               cursor: page.pagination.next_cursor,
               limit: 1
             )

    assert next_page.summary.evidence_generation_id == "semantic-a"
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

  # Submitting re-plans from the selection it was handed and refuses unless the hash
  # still matches, so an explicit selection has to survive the trip out to a browser and
  # back with its keys intact. If it did not, every backfill an operator picked on the
  # calendar would come back `:coverage_selection_stale`: the plan would be rebuilt as
  # *all* missing windows and disagree with the one they reviewed.
  #
  # This stops at revalidation rather than completing a submission, because the rest of
  # `submit_missing_backfill/4` is the durable backfill queue, and faking that here
  # would be a test of the fake. Checked separately that an explicit plan does reach the
  # queue: it clears the hash, manifest, and deployment comparisons and stops only at
  # the store this fixture does not implement.
  test "a selection an operator named survives the round trip out to the client", fixture do
    assert {:ok, all} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    [_first, second, third] = all.windows

    assert {:ok, plan} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               window_keys: [third.window_key, second.window_key]
             )

    # The selection as the client hands it back: string keys, no atoms.
    selection = plan.selection |> Jason.encode!() |> Jason.decode!()

    assert {:ok, resubmitted} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: plan.evaluated_at,
               window_keys: selection["window_keys"]
             )

    assert resubmitted.plan_hash == plan.plan_hash
    assert resubmitted.plan_id == plan.plan_id
    assert Enum.map(resubmitted.windows, & &1.start_at.day) == [2, 3]
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

  test "can freeze exactly the windows an operator named", fixture do
    assert {:ok, all} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    [_first, second, third] = all.windows

    assert {:ok, plan} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               window_keys: [third.window_key, second.window_key]
             )

    # Sorted, so the plan hash depends on which windows were chosen rather than the
    # order they were clicked in.
    assert plan.selection == %{
             mode: :explicit,
             window_keys: Enum.sort([second.window_key, third.window_key])
           }

    assert plan.window_count == 2
    assert Enum.map(plan.windows, & &1.start_at.day) == [2, 3]
    assert plan.plan_hash != all.plan_hash
  end

  test "an explicit plan survives a round trip and rejects a window that filled in",
       fixture do
    assert {:ok, all} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at
             )

    [first, second, _third] = all.windows

    assert {:ok, plan} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               window_keys: [first.window_key, second.window_key]
             )

    # The plan the operator reviewed round-trips through the LiveView untouched, so
    # revalidation has to accept it byte for byte before anything is submitted.
    assert {:ok, revalidated} =
             Coverage.plan_missing_backfill(
               fixture.context,
               fixture.target_id,
               evaluated_at: plan.evaluated_at,
               window_keys: plan.selection.window_keys
             )

    assert revalidated.plan_hash == plan.plan_hash

    # One of the two is covered now. Planning the remaining one would run something
    # other than what was reviewed, so the whole selection is refused.
    Process.put(:coverage_count_result, {:ok, 1})
    Process.put(:coverage_successful_keys, ["window:" <> first.window_key])

    assert {:error, :coverage_selection_stale} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               evaluated_at: @evaluated_at,
               window_keys: [first.window_key, second.window_key]
             )
  end

  test "rejects a window selection that is empty, duplicated, or paged as well",
       fixture do
    assert {:error, :invalid_coverage_window_selection} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id, window_keys: [])

    assert {:error, :invalid_coverage_window_selection} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               window_keys: ["day:Etc/UTC:2026-07-01", "day:Etc/UTC:2026-07-01"]
             )

    assert {:error, :invalid_coverage_window_selection} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               window_keys: [:not_a_key]
             )

    # A selection and a page are two different answers to "which windows", so asking
    # for both is a caller bug rather than a precedence question.
    assert {:error, :invalid_coverage_options} =
             Coverage.plan_missing_backfill(fixture.context, fixture.target_id,
               window_keys: ["day:Etc/UTC:2026-07-01"],
               limit: 2
             )
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

  defp coverage(from \\ ~D[2026-07-01], through \\ ~D[2026-07-03]) do
    window = WindowSpec.new!(:day, timezone: "Etc/UTC")

    {:ok, coverage} = Effective.resolve(Spec.new!(from: from, through: through), window, nil)

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
      schema_version: 17,
      runner_contract_version: 13,
      runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
      manifest: %Manifest{
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        assets: [asset]
      }
    }
  end
end

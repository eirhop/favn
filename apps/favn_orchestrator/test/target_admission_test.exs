defmodule FavnOrchestrator.TargetAdmissionTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.TargetIdentity
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetAdmission
  alias FavnOrchestrator.TargetGenerations

  @now ~U[2026-07-22 12:00:00Z]

  defmodule FakeStore do
    def get_bindings(query) do
      send(self(), {:get_bindings, query.target_ids})
      {:ok, Process.get(:target_admission_bindings, [])}
    end

    def ensure_writable(command) do
      send(self(), {:ensure_writable, command.target_id})
      {:error, :unexpected_generation_creation}
    end
  end

  setup do
    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
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

    Process.put(:target_admission_bindings, [])

    {:ok, context} =
      WorkspaceContext.new("workspace", "target-admission-test", [:workspace_admin])

    {:ok, context: context}
  end

  test "allows every ordinary-write compatibility state" do
    ref = {MyApp.Ready, :asset}
    plan = plan([ref], [], [ref])

    for status <- [:ready, :uninitialized, :rebuild_available] do
      assert :ok = TargetAdmission.check(plan, [binding(ref, status)])
    end

    assert :ok = TargetAdmission.check(plan, [])
  end

  test "returns stable error codes and the exact dependent path" do
    upstream = {MyApp.Blocked, :asset}
    middle = {MyApp.Middle, :asset}
    target = {MyApp.Target, :asset}
    plan = plan([upstream, middle, target], [{upstream, middle}, {middle, target}], [target])

    for {status, error_code} <- [
          rebuild_required: :rebuild_required,
          unexpected_drift: :target_drift,
          operator_decision: :operator_decision_required
        ] do
      assert {:error, {^error_code, details}} =
               TargetAdmission.check(plan, [binding(upstream, status)])

      assert details.target_id == TargetIdentity.for_asset(upstream)
      assert details.selected_target_id == TargetIdentity.for_asset(target)

      assert details.blocked_path ==
               Enum.map([upstream, middle, target], &TargetIdentity.for_asset/1)

      assert details.blocked_path_target_count == 3
      refute details.blocked_path_truncated
      assert details.compatibility_status == status
      assert details.reason_code == "reason-#{status}"
    end
  end

  test "ignores bindings outside the selected plan" do
    selected = {MyApp.Selected, :asset}
    unrelated = {MyApp.Unrelated, :asset}

    assert :ok =
             TargetAdmission.check(
               plan([selected], [], [selected]),
               [binding(unrelated, :rebuild_required)]
             )
  end

  test "chooses the blocked target deterministically" do
    first = {MyApp.Alpha, :asset}
    second = {MyApp.Zulu, :asset}
    plan = plan([first, second], [], [first, second])
    bindings = [binding(second, :operator_decision), binding(first, :unexpected_drift)]

    assert {:error, {:target_drift, details}} = TargetAdmission.check(plan, bindings)
    assert details.target_id == TargetIdentity.for_asset(first)

    assert {:error, {:target_drift, ^details}} =
             TargetAdmission.check(plan, Enum.reverse(bindings))
  end

  test "preflights the complete selected path before creating a building generation", %{
    context: context
  } do
    blocked = {MyApp.Blocked, :asset}
    target = {MyApp.Target, :asset}
    unrelated = {MyApp.Unrelated, :asset}
    plan = plan([blocked, target], [{blocked, target}], [target])

    index = %Index{
      assets_by_ref: %{
        blocked => persisted_asset(blocked),
        target => persisted_asset(target),
        unrelated => persisted_asset(unrelated)
      }
    }

    version = %Version{manifest_version_id: "manifest", required_runner_release_id: "runner"}

    Process.put(:target_admission_bindings, [
      binding(blocked, :rebuild_required),
      binding(target, :ready)
    ])

    assert {:error, {:rebuild_required, details}} =
             TargetGenerations.pin_plan(context, version, index, plan, @now)

    assert details.blocked_path ==
             Enum.map([blocked, target], &TargetIdentity.for_asset/1)

    selected_ids = Enum.map([blocked, target], &TargetIdentity.for_asset/1) |> Enum.sort()
    assert_received {:get_bindings, ^selected_ids}
    refute_received {:ensure_writable, _target_id}
  end

  test "blocks a persisted target whose deployment binding is missing", %{context: context} do
    target = {MyApp.Unbound, :asset}
    plan = plan([target], [], [target])
    index = %Index{assets_by_ref: %{target => persisted_asset(target)}}

    assert {:error, {:operator_decision_required, details}} =
             TargetAdmission.preflight(context, index, plan)

    assert details.target_id == TargetIdentity.for_asset(target)
    assert details.reason_code == "target_binding_missing"
    assert details.compatibility_status == :operator_decision
  end

  defp plan(refs, edges, target_refs) do
    nodes =
      Map.new(refs, fn ref ->
        upstream = for {parent, ^ref} <- edges, do: node_key(parent)
        downstream = for {^ref, child} <- edges, do: node_key(child)

        {node_key(ref),
         %{
           ref: ref,
           node_key: node_key(ref),
           upstream: upstream,
           downstream: downstream
         }}
      end)

    %Plan{
      target_refs: target_refs,
      target_node_keys: Enum.map(target_refs, &node_key/1),
      nodes: nodes
    }
  end

  defp persisted_asset(ref) do
    descriptor = struct(TargetDescriptor, target_id: TargetIdentity.for_asset(ref))
    %Asset{ref: ref, target_descriptor: descriptor}
  end

  defp binding(ref, status) do
    %TargetBinding{
      workspace_id: "workspace",
      target_id: TargetIdentity.for_asset(ref),
      active_generation_id: nil,
      desired_manifest_id: "manifest",
      desired_descriptor_hash: String.duplicate("a", 64),
      compatibility_status: status,
      reason_code: "reason-#{status}",
      compatibility_diff: %{},
      active_physical_fingerprint: nil,
      version: 1,
      updated_at: @now
    }
  end

  defp node_key(ref), do: {ref, nil}
end

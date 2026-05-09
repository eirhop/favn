defmodule FavnOrchestrator.RefreshPolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.RefreshPolicy

  @raw {MyApp.Assets.Raw, :asset}
  @staged {MyApp.Assets.Staged, :asset}
  @gold {MyApp.Assets.Gold, :asset}
  @unplanned {MyApp.Assets.Unplanned, :asset}

  test "normalizes automatic refresh policy from nil and explicit modes" do
    assert {:ok, %RefreshPolicy{mode: :auto, refs: [], include_upstream?: false}} =
             RefreshPolicy.from_value(nil)

    assert {:ok, %RefreshPolicy{mode: :force}} = RefreshPolicy.from_value(:force)
    assert {:ok, %RefreshPolicy{mode: :missing}} = RefreshPolicy.from_value("missing")
    assert {:ok, %RefreshPolicy{mode: :auto}} = RefreshPolicy.from_opts(refresh_policy: :auto)
    assert {:ok, %RefreshPolicy{mode: :force}} = RefreshPolicy.from_opts(%{refresh: "force"})
  end

  test "normalizes selected force assets" do
    assert {:ok, %RefreshPolicy{mode: :force_assets, refs: [@gold], include_upstream?: false}} =
             RefreshPolicy.from_value({:force_assets, [@gold]})

    assert {:ok, %RefreshPolicy{mode: :force_assets, refs: [@gold], include_upstream?: true}} =
             RefreshPolicy.from_value({:force_assets, [@gold], include_upstream: true})

    assert {:ok, %RefreshPolicy{mode: :force_assets, refs: [@gold], include_upstream?: true}} =
             RefreshPolicy.from_value(%{
               mode: :force_assets,
               refs: [@gold],
               include_upstream?: true
             })
  end

  test "rejects invalid refs and options" do
    assert {:error, {:invalid_refresh_ref, {MyApp.Assets.Gold, "asset"}}} =
             RefreshPolicy.from_value({:force_assets, [{MyApp.Assets.Gold, "asset"}]})

    assert {:error, {:invalid_refresh_refs, @gold}} =
             RefreshPolicy.from_value({:force_assets, @gold})

    assert {:error, {:invalid_include_upstream, :yes}} =
             RefreshPolicy.from_value({:force_assets, [@gold], include_upstream: :yes})

    assert {:error, {:invalid_refresh_policy, :later}} = RefreshPolicy.from_value(:later)
    assert_raise ArgumentError, fn -> RefreshPolicy.from_value!(:later) end
  end

  test "force expands to all planned node keys" do
    plan = plan()
    policy = RefreshPolicy.from_value!(:force)

    assert RefreshPolicy.expand_force_set(policy, plan) ==
             MapSet.new([{@raw, nil}, {@staged, nil}, {@gold, nil}])
  end

  test "force_assets expands only matching planned asset node keys" do
    plan = plan()
    policy = RefreshPolicy.from_value!({:force_assets, [@gold, @unplanned]})

    assert RefreshPolicy.expand_force_set(policy, plan) == MapSet.new([{@gold, nil}])
  end

  test "selected downstream force does not force upstream by default" do
    plan = plan()
    policy = RefreshPolicy.from_value!({:force_assets, [@gold]})

    assert RefreshPolicy.expand_force_set(policy, plan) == MapSet.new([{@gold, nil}])
  end

  test "selected force asset expands transitive upstream within planned graph" do
    plan = plan_with_external_upstream_edge()
    policy = RefreshPolicy.from_value!({:force_assets, [@gold], include_upstream: true})

    assert RefreshPolicy.expand_force_set(policy, plan) ==
             MapSet.new([{@raw, nil}, {@staged, nil}, {@gold, nil}])
  end

  test "auto and missing expand to empty forced sets" do
    plan = plan()

    assert RefreshPolicy.expand_force_set(RefreshPolicy.from_value!(:auto), plan) == MapSet.new()

    assert RefreshPolicy.expand_force_set(RefreshPolicy.from_value!(:missing), plan) ==
             MapSet.new()
  end

  defp plan do
    %Plan{
      target_refs: [@gold],
      target_node_keys: [{@gold, nil}],
      nodes: %{
        {@raw, nil} => node(@raw, upstream: [], downstream: [{@staged, nil}], stage: 0),
        {@staged, nil} =>
          node(@staged, upstream: [{@raw, nil}], downstream: [{@gold, nil}], stage: 1),
        {@gold, nil} => node(@gold, upstream: [{@staged, nil}], downstream: [], stage: 2)
      },
      topo_order: [@raw, @staged, @gold],
      stages: [[@raw], [@staged], [@gold]],
      node_stages: [[{@raw, nil}], [{@staged, nil}], [{@gold, nil}]]
    }
  end

  defp plan_with_external_upstream_edge do
    update_in(plan().nodes[{@raw, nil}].upstream, &[{@unplanned, nil} | &1])
  end

  defp node(ref, opts) do
    %{
      ref: ref,
      node_key: {ref, nil},
      window: nil,
      upstream: Keyword.fetch!(opts, :upstream),
      downstream: Keyword.fetch!(opts, :downstream),
      stage: Keyword.fetch!(opts, :stage),
      action: :run
    }
  end
end

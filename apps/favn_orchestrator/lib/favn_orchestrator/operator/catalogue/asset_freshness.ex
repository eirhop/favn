defmodule FavnOrchestrator.Operator.Catalogue.AssetFreshness do
  @moduledoc """
  Explains operator-facing asset freshness from manifest policy and persisted state.

  The result is a read model, not a second freshness engine: decisions are
  delegated to the runtime freshness decider using the same planning index and
  state keys used during execution.
  """

  alias Favn.Assets.Planner
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.TimePeriod
  alias Favn.Window.Anchor
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Freshness.Decider, as: FreshnessDecider
  alias FavnOrchestrator.Operator.Catalogue.Targets

  @doc "Builds an operator freshness explanation for one manifest asset."
  @spec detail(Asset.t(), Version.t(), [AssetFreshnessState.t()], keyword()) :: map()
  def detail(%Asset{} = asset, %Version{} = version, freshness_states, opts)
      when is_list(freshness_states) and is_list(opts) do
    policy = policy(asset)
    now = Keyword.get(opts, :now) || DateTime.utc_now()
    opts = normalize_run_context_opts(version, asset, opts)

    case {policy.kind, opts[:run_context_status]} do
      {kind, :ambiguous} when kind not in [:always, :none] ->
        run_context_required_detail(policy)

      {:always, _status} ->
        freshness_detail(
          :always_run,
          policy,
          nil,
          "Freshness is intentionally bypassed; this asset runs whenever it is planned.",
          [%{kind: :always_run, message: "Manifest policy is always run."}]
        )

      {:none, _status} ->
        freshness_detail(
          :unknown,
          policy,
          nil,
          "No freshness policy is declared for this asset.",
          [%{kind: :no_freshness_policy, message: "No freshness policy is declared."}]
        )

      {_policy_kind, _status} ->
        classify(asset, version, freshness_states, policy, now, opts)
    end
  end

  @doc "Returns the latest-key freshness state for a canonical asset ref string."
  @spec latest_for_ref([AssetFreshnessState.t()], String.t()) :: AssetFreshnessState.t() | nil
  def latest_for_ref(freshness_states, ref_string)
      when is_list(freshness_states) and is_binary(ref_string) do
    Enum.find(freshness_states, fn
      %AssetFreshnessState{} = state ->
        ref_string(state) == ref_string and state.freshness_key == FreshnessKey.latest()

      _state ->
        false
    end)
  end

  @doc "Returns the canonical asset ref string stored in a freshness state."
  @spec ref_string(AssetFreshnessState.t()) :: String.t()
  def ref_string(%AssetFreshnessState{} = state) do
    Targets.ref_string({state.asset_ref_module, state.asset_ref_name})
  end

  defp classify(asset, version, freshness_states, policy, now, opts) do
    with {:ok, plan} <- freshness_plan(asset, version, now, opts),
         {:ok, target_node_keys} <- target_node_keys(plan, asset.ref) do
      states = state_lookup(freshness_states)
      assets_by_ref = Map.new(List.wrap(version.manifest.assets), &{&1.ref, &1})

      decisions =
        FreshnessDecider.decide_many(plan, target_node_keys,
          assets_by_ref: assets_by_ref,
          prior_states: states,
          current_states: states,
          now: now
        )

      decision_entries =
        Enum.map(target_node_keys, fn node_key ->
          decision = Map.fetch!(decisions, node_key)
          {decision, decision_state(states, asset.ref, decision)}
        end)

      detail_from_decisions(asset, policy, decision_entries)
    else
      {:error, _reason} -> insufficient_state_detail(policy)
    end
  end

  defp detail_from_decision(_asset, policy, state, %{decision: :skipped_fresh}) do
    freshness_detail(
      :fresh,
      policy,
      latest_success_detail(state),
      "Backend freshness state currently satisfies this asset's policy.",
      [%{kind: :policy_fresh, message: "Backend freshness state satisfies the declared policy."}]
    )
  end

  defp detail_from_decision(_asset, policy, state, %{decision: :run})
       when is_nil(state) or is_nil(state.latest_success_run_id) do
    freshness_detail(
      :unknown,
      policy,
      nil,
      "No successful freshness evidence exists for this asset yet.",
      [
        %{
          kind: :never_run,
          message: "No successful freshness-producing run has been recorded."
        }
      ]
    )
  end

  defp detail_from_decision(asset, policy, state, %{
         decision: :run,
         reason: :upstream_version_changed,
         stale_reasons: stale_reasons
       }) do
    reasons = Enum.map(stale_reasons, &freshness_reason/1)

    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      stale_explanation(asset, reasons),
      reasons
    )
  end

  defp detail_from_decision(_asset, policy, state, %{
         decision: :run,
         reason: :freshness_expired
       }) do
    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      "Stored freshness evidence no longer satisfies this asset's policy.",
      [
        %{
          kind: :freshness_expired,
          message: "Stored freshness evidence no longer satisfies the declared policy."
        }
      ]
    )
  end

  defp detail_from_decision(_asset, policy, state, %{decision: :run, reason: reason}) do
    freshness_detail(
      :stale,
      policy,
      latest_success_detail(state),
      "Backend freshness policy requires this asset to run.",
      [%{kind: reason, message: "Backend freshness policy requires this asset to run."}]
    )
  end

  defp detail_from_decision(_asset, policy, _state, _decision),
    do: insufficient_state_detail(policy)

  defp freshness_plan(asset, version, now, opts) do
    with {:ok, index} <- ManifestIndexCache.fetch(version) do
      planner_opts = [dependencies: :all, planning_index: index.planning_index]

      planner_opts =
        case current_anchor_window(asset, now, opts) do
          {:ok, anchor_window} -> Keyword.put(planner_opts, :anchor_window, anchor_window)
          :error -> planner_opts
        end

      Planner.plan(asset.ref, planner_opts)
    end
  end

  defp current_anchor_window(asset, now, opts) do
    with %AssetRunContext{} = run_context <- Keyword.get(opts, :asset_run_context),
         {:ok, %Anchor{} = anchor} <- AssetRunContext.anchor(run_context, now) do
      {:ok, anchor}
    else
      _error -> asset_anchor_window(asset, now)
    end
  end

  defp normalize_run_context_opts(version, asset, opts) do
    if Keyword.has_key?(opts, :run_context_status) do
      opts
    else
      case AssetRunContext.select(version, asset) do
        {:ok, selection} ->
          opts
          |> Keyword.put(:asset_run_context, selection.selected)
          |> Keyword.put(:run_context_status, selection.status)

        {:error, _reason} ->
          opts
          |> Keyword.put(:asset_run_context, nil)
          |> Keyword.put(:run_context_status, :unavailable)
      end
    end
  end

  defp asset_anchor_window(%{window: %WindowSpec{} = spec}, now) do
    with {:ok, period} <- TimePeriod.current(spec.kind, now, spec.timezone) do
      {:ok, Anchor.new!(period.kind, period.start_at, period.end_at, timezone: period.timezone)}
    end
  end

  defp asset_anchor_window(_asset, _now), do: :error

  defp target_node_keys(plan, asset_ref) do
    target_node_keys =
      Enum.filter(plan.target_node_keys, fn {ref, _window_key} -> ref == asset_ref end)

    case target_node_keys do
      [] -> {:error, :target_node_key_not_found}
      target_node_keys -> {:ok, target_node_keys}
    end
  end

  defp detail_from_decisions(asset, policy, decision_entries) do
    case Enum.find(decision_entries, fn {decision, _state} ->
           decision.decision != :skipped_fresh
         end) do
      nil ->
        {_decision, state} =
          Enum.max_by(
            decision_entries,
            fn {_decision, state} -> state_timestamp(state) end,
            &>=/2
          )

        detail_from_decision(asset, policy, state, %{decision: :skipped_fresh})

      {decision, state} ->
        detail_from_decision(asset, policy, state, decision)
    end
  end

  defp state_timestamp(%AssetFreshnessState{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp state_timestamp(_state), do: 0

  defp state_lookup(freshness_states) do
    Enum.reduce(freshness_states, %{}, fn
      %AssetFreshnessState{} = state, acc ->
        ref = {state.asset_ref_module, state.asset_ref_name}

        acc
        |> maybe_put(state.latest_success_node_key, state)
        |> Map.put({ref, state.freshness_key}, state)
        |> Map.put(Targets.ref_string(ref) <> ":" <> state.freshness_key, state)

      _state, acc ->
        acc
    end)
  end

  defp decision_state(states, asset_ref, decision) do
    case Map.get(decision, :freshness_key) do
      nil -> nil
      freshness_key -> Map.get(states, {asset_ref, freshness_key})
    end
  end

  defp freshness_detail(state, policy, latest_success, explanation, reasons) do
    %{
      state: state,
      policy: policy,
      latest_success: latest_success,
      explanation: explanation,
      reasons: reasons
    }
  end

  defp insufficient_state_detail(policy) do
    freshness_detail(
      :unknown,
      policy,
      nil,
      "Freshness state exists, but backend could not explain whether it is stale.",
      [
        %{
          kind: :insufficient_state,
          message: "Backend could not build a staleness explanation from available state."
        }
      ]
    )
  end

  defp run_context_required_detail(policy) do
    freshness_detail(
      :unknown,
      policy,
      nil,
      "Select a pipeline context before evaluating policy-sensitive freshness.",
      [
        %{
          kind: :run_context_required,
          message: "Multiple pipeline policies select this asset."
        }
      ]
    )
  end

  defp latest_success_detail(nil), do: nil
  defp latest_success_detail(%AssetFreshnessState{latest_success_run_id: nil}), do: nil

  defp latest_success_detail(%AssetFreshnessState{} = state) do
    %{
      run_id: state.latest_success_run_id,
      at: state.latest_success_at,
      freshness_key: state.freshness_key
    }
  end

  defp policy(%{freshness: nil}), do: %{kind: :none, label: "no freshness policy"}

  defp policy(%{
         freshness: %FreshnessPolicy{mode: :calendar_period, kind: :day, timezone: timezone}
       }),
       do: %{kind: :daily, label: "daily #{timezone || "Etc/UTC"}"}

  defp policy(%{freshness: %FreshnessPolicy{mode: :max_age, amount: amount, unit: unit}}),
    do: %{kind: :max_age, label: "max age #{amount} #{pluralize(unit, amount)}"}

  defp policy(%{freshness: %FreshnessPolicy{mode: :window_success}}),
    do: %{kind: :window_success, label: "window success"}

  defp policy(%{freshness: %FreshnessPolicy{mode: :always}}),
    do: %{kind: :always, label: "always run"}

  defp policy(_asset), do: %{kind: :none, label: "no freshness policy"}

  defp freshness_reason(%{type: :upstream_version_changed} = reason) do
    upstream_ref = Map.get(reason, :upstream_ref)

    %{
      kind: :upstream_version_changed,
      message: "#{ref_display_name(upstream_ref)} refreshed after this asset last consumed it.",
      upstream_ref: Targets.ref_string(upstream_ref),
      previous_version: Map.get(reason, :consumed_version),
      current_version: Map.get(reason, :current_version),
      run_id: Map.get(reason, :current_success_run_id)
    }
  end

  defp freshness_reason(%{type: :missing_upstream_version} = reason) do
    upstream_ref = Map.get(reason, :upstream_ref)

    %{
      kind: :upstream_missing,
      message:
        "#{ref_display_name(upstream_ref)} has no current upstream freshness version available.",
      upstream_ref: Targets.ref_string(upstream_ref),
      previous_version: Map.get(reason, :consumed_version),
      current_version: nil,
      run_id: nil
    }
  end

  defp freshness_reason(reason) do
    %{
      kind: :unknown,
      message: "Backend returned an unrecognized stale reason: #{inspect(reason, limit: 5)}."
    }
  end

  defp stale_explanation(asset, []),
    do: "#{ref_display_name(asset.ref)} is stale according to backend freshness state."

  defp stale_explanation(asset, [reason | _reasons]) do
    "#{ref_display_name(asset.ref)} is stale because #{String.downcase(reason.message)}"
  end

  defp ref_display_name({module, name}) when is_atom(module) and is_atom(name) do
    module
    |> inspect()
    |> String.split(".")
    |> List.last()
    |> Kernel.<>(".#{name}")
  end

  defp ref_display_name(ref), do: Targets.ref_string(ref)

  defp pluralize(unit, 1), do: to_string(unit)
  defp pluralize(unit, _amount), do: to_string(unit) <> "s"

  defp maybe_put(map, nil, _value), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end

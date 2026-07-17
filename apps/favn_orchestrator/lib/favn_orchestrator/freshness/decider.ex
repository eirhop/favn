defmodule FavnOrchestrator.Freshness.Decider do
  @moduledoc """
  Pure execution-time freshness decisions for planned nodes.

  This module does not read storage or execute work. Callers provide the plan,
  manifest freshness metadata, prior/current freshness states, current run
  outcomes, and the decision time.
  """

  alias Favn.Freshness.{Key, Policy}
  alias Favn.{Plan, TimePeriod}
  alias Favn.Window.Spec
  alias FavnOrchestrator.{AssetFreshnessState, RefreshPolicy}
  alias FavnOrchestrator.Freshness.Staleness

  @blocking_statuses [:error, :cancelled, :timed_out, :blocked]
  @type decision :: %{
          required(:decision) => :run | :skipped_fresh | :blocked,
          required(:reason) => atom(),
          optional(atom()) => term()
        }

  @type opts :: [
          assets_by_ref: map(),
          refresh_policy: RefreshPolicy.t() | RefreshPolicy.mode() | nil,
          forced_node_keys: MapSet.t(Plan.node_key()) | [Plan.node_key()],
          prior_states: map(),
          current_states: map(),
          completed_node_keys: MapSet.t(Plan.node_key()) | [Plan.node_key()],
          refreshed_node_keys: MapSet.t(Plan.node_key()) | [Plan.node_key()],
          upstream_statuses: map(),
          now: DateTime.t()
        ]

  @doc """
  Decides whether a single planned node should run, skip as fresh, or block.
  """
  @spec decide(Plan.t(), Plan.node_key(), opts()) :: decision()
  def decide(%Plan{} = plan, node_key, opts \\ []) do
    node = Map.fetch!(plan.nodes, node_key)
    context = context(plan, opts)

    with :ok <- dependency_satisfied?(node, context) do
      cond do
        MapSet.member?(context.forced_node_keys, node_key) ->
          run(:forced, node, context)

        upstream_refreshed?(node, context) ->
          run(:upstream_refreshed, node, context)

        true ->
          stale_or_fresh_decision(node, context)
      end
    end
  end

  @doc """
  Decides a list of planned node keys, preserving input order.
  """
  @spec decide_many(Plan.t(), [Plan.node_key()], opts()) :: %{
          required(Plan.node_key()) => decision()
        }
  def decide_many(%Plan{} = plan, node_keys, opts \\ []) when is_list(node_keys) do
    Map.new(node_keys, &{&1, decide(plan, &1, opts)})
  end

  @doc """
  Returns the persisted freshness-state keys a plan may need for decisions.

  The helper shares the same key derivation used by `decide/3`, allowing run
  startup to fetch only relevant prior freshness rows from storage.
  """
  @spec planned_lookup_keys(Plan.t(), opts()) :: [{module(), atom(), String.t()}]
  def planned_lookup_keys(%Plan{} = plan, opts \\ []) do
    context = context(plan, opts)

    plan.nodes
    |> Map.values()
    |> Enum.map(fn %{ref: {module, name}} = node ->
      {module, name, freshness_key(node, context)}
    end)
    |> Enum.uniq()
  end

  defp dependency_satisfied?(%{upstream: upstream}, context) do
    blocking =
      Enum.filter(upstream, &(Map.get(context.upstream_statuses, &1) in @blocking_statuses))

    case blocking do
      [] ->
        :ok

      blocking ->
        %{decision: :blocked, reason: :upstream_blocked, blocking_upstream: blocking}
    end
  end

  defp upstream_refreshed?(%{upstream: upstream}, context) do
    Enum.any?(upstream, fn upstream_node_key ->
      MapSet.member?(context.refreshed_node_keys, upstream_node_key)
    end)
  end

  defp stale_or_fresh_decision(node, context) do
    state = own_state(node, context)

    if context.refresh_policy.mode == :missing and successful_state?(state) do
      skipped(:existing_success, node, context)
    else
      current_upstream_states = current_upstream_states(node, context)

      case Staleness.freshness(state || %{}, node, current_upstream_states) do
        {:stale, reasons} ->
          run(:upstream_version_changed, node, context, %{stale_reasons: reasons})

        :fresh ->
          freshness_policy_decision(node, state, context)
      end
    end
  end

  defp freshness_policy_decision(node, state, context) do
    policy = freshness_policy(node, context)

    cond do
      is_nil(policy) ->
        run(:no_freshness_policy, node, context)

      policy.mode == :always ->
        run(:always, node, context)

      fresh_by_policy?(policy, node, state, context) ->
        skipped(policy.mode, node, context)

      true ->
        run(:freshness_expired, node, context)
    end
  end

  defp fresh_by_policy?(%Policy{mode: :window_success}, _node, state, _context) do
    successful_state?(state) and not is_nil(freshness_version(state))
  end

  defp fresh_by_policy?(%Policy{mode: :calendar_period}, _node, state, _context) do
    successful_state?(state) and not is_nil(freshness_version(state))
  end

  defp fresh_by_policy?(
         %Policy{mode: :max_age, amount: amount, unit: unit},
         _node,
         state,
         context
       ) do
    with true <- successful_state?(state),
         %DateTime{} = latest_success_at <- latest_success_at(state),
         seconds <- max_age_seconds(amount, unit) do
      DateTime.compare(latest_success_at, DateTime.add(context.now, -seconds, :second)) != :lt
    else
      _other -> false
    end
  end

  defp fresh_by_policy?(_policy, _node, _state, _context), do: false

  defp own_state(node, context) do
    freshness_key = freshness_key(node, context)

    get_state(context.prior_states, node.node_key) ||
      get_state(context.prior_states, {node.ref, freshness_key}) ||
      get_state(context.prior_states, freshness_key) ||
      get_state(context.current_states, node.node_key) ||
      get_state(context.current_states, {node.ref, freshness_key}) ||
      get_state(context.current_states, freshness_key)
  end

  defp current_upstream_states(%{upstream: upstream}, context) do
    Enum.reduce(upstream, %{}, fn upstream_node_key, acc ->
      upstream_node = Map.fetch!(context.plan.nodes, upstream_node_key)
      freshness_key = freshness_key(upstream_node, context)

      state =
        get_state(context.current_states, upstream_node_key) ||
          get_state(context.current_states, {upstream_node.ref, freshness_key}) ||
          get_state(context.current_states, freshness_key) ||
          get_state(context.prior_states, upstream_node_key) ||
          get_state(context.prior_states, {upstream_node.ref, freshness_key}) ||
          get_state(context.prior_states, freshness_key)

      if is_nil(state), do: acc, else: Map.put(acc, upstream_node_key, state)
    end)
  end

  defp freshness_key(node, context) do
    case freshness_policy(node, context) do
      %Policy{mode: :window_success} ->
        window_freshness_key(node, context)

      %Policy{mode: :calendar_period, kind: kind, timezone: timezone} ->
        calendar_freshness_key(kind, timezone, context.now)

      _policy ->
        Key.latest()
    end
  end

  defp window_freshness_key(%{ref: ref, window: %{key: window_key}}, context) do
    case window_spec(ref, context) do
      %Spec{refresh_from: refresh_from, timezone: timezone} when not is_nil(refresh_from) ->
        period = TimePeriod.current(refresh_from, context.now, timezone) |> elem(1)
        Key.window_refresh!(window_key, refresh_from, timezone, period.start_at)

      _other ->
        Key.window!(window_key)
    end
  end

  defp window_freshness_key(_node, _context), do: Key.latest()

  defp window_spec(ref, context) do
    value = context.assets_by_ref |> Map.get(ref, %{}) |> field(:window)

    case Spec.from_value(value) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid window spec: #{inspect(reason)}"
    end
  end

  defp calendar_freshness_key(kind, timezone, now) do
    period = TimePeriod.current(kind, now, timezone) |> elem(1)
    Key.calendar!(kind, timezone, period.start_at)
  end

  defp freshness_policy(%{ref: ref}, context) do
    context.assets_by_ref
    |> Map.get(ref, %{})
    |> field(:freshness)
    |> normalize_freshness_policy()
  end

  defp normalize_freshness_policy(%Policy{} = policy), do: policy
  defp normalize_freshness_policy(nil), do: nil

  defp normalize_freshness_policy(value) do
    case Policy.from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid freshness policy: #{inspect(reason)}"
    end
  end

  defp context(plan, opts) do
    refresh_policy = refresh_policy(Keyword.get(opts, :refresh_policy))

    forced_node_keys =
      opts
      |> Keyword.get(:forced_node_keys, RefreshPolicy.expand_force_set(refresh_policy, plan))
      |> set()

    %{
      plan: plan,
      assets_by_ref: Keyword.get(opts, :assets_by_ref, %{}),
      refresh_policy: refresh_policy,
      forced_node_keys: forced_node_keys,
      prior_states: Keyword.get(opts, :prior_states, %{}),
      current_states: Keyword.get(opts, :current_states, %{}),
      completed_node_keys: opts |> Keyword.get(:completed_node_keys, []) |> set(),
      refreshed_node_keys: opts |> Keyword.get(:refreshed_node_keys, []) |> set(),
      upstream_statuses: Keyword.get(opts, :upstream_statuses, %{}),
      now: Keyword.fetch!(opts, :now)
    }
  end

  defp refresh_policy(nil), do: %RefreshPolicy{mode: :auto}
  defp refresh_policy(%RefreshPolicy{} = policy), do: policy

  defp refresh_policy(value) do
    case RefreshPolicy.from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid refresh policy: #{inspect(reason)}"
    end
  end

  defp set(%MapSet{} = set), do: set
  defp set(list) when is_list(list), do: MapSet.new(list)

  defp run(reason, node, context, extra \\ %{}) do
    Map.merge(
      %{
        decision: :run,
        reason: reason,
        node_key: node.node_key,
        freshness_key: freshness_key(node, context)
      },
      extra
    )
  end

  defp skipped(reason, node, context) do
    %{
      decision: :skipped_fresh,
      reason: reason,
      node_key: node.node_key,
      freshness_key: freshness_key(node, context)
    }
  end

  defp successful_state?(%AssetFreshnessState{} = state),
    do: not is_nil(state.freshness_version) and not is_nil(state.latest_success_at)

  defp successful_state?(%{} = state),
    do:
      not is_nil(field(state, :freshness_version)) and
        not is_nil(field(state, :latest_success_at))

  defp successful_state?(_state), do: false

  defp freshness_version(%AssetFreshnessState{} = state), do: state.freshness_version
  defp freshness_version(%{} = state), do: field(state, :freshness_version)

  defp latest_success_at(%AssetFreshnessState{} = state), do: state.latest_success_at
  defp latest_success_at(%{} = state), do: field(state, :latest_success_at)

  defp get_state(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, inspect(key))
  defp get_state(_map, _key), do: nil

  defp field(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp field(_value, _key), do: nil

  defp max_age_seconds(amount, :second), do: amount
  defp max_age_seconds(amount, :minute), do: amount * 60
  defp max_age_seconds(amount, :hour), do: amount * 60 * 60
  defp max_age_seconds(amount, :day), do: amount * 24 * 60 * 60
end

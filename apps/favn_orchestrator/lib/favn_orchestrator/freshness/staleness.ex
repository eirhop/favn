defmodule FavnOrchestrator.Freshness.Staleness do
  @moduledoc """
  Pure helpers for deriving consumed input versions and stale reasons.

  The helpers compare upstream freshness versions, not timestamps. They do not
  read storage or decide execution policy; callers provide the planned upstream
  node keys and the current upstream freshness states.
  """

  alias FavnOrchestrator.AssetFreshnessState

  @type input_version :: %{
          upstream_ref: Favn.Ref.t(),
          upstream_node_key: Favn.Plan.node_key(),
          freshness_version: String.t() | nil,
          success_run_id: String.t() | nil
        }

  @type stale_reason :: %{
          type: :missing_upstream_version | :upstream_version_changed,
          upstream_ref: Favn.Ref.t(),
          upstream_node_key: Favn.Plan.node_key(),
          consumed_version: String.t() | nil,
          current_version: String.t() | nil,
          current_success_run_id: String.t() | nil
        }

  @doc """
  Builds list-shaped input versions consumed by a planned node.

  The first argument may be a plan node map with an `:upstream` list or the
  upstream node key list directly. Missing upstream states are represented with a
  `nil` freshness version so the result stays deterministic and serializable.
  """
  @spec consumed_input_versions(Favn.Plan.plan_node() | [Favn.Plan.node_key()], map()) :: [
          input_version()
        ]
  def consumed_input_versions(%{upstream: upstream_node_keys}, current_upstream_states),
    do: consumed_input_versions(upstream_node_keys, current_upstream_states)

  def consumed_input_versions(upstream_node_keys, current_upstream_states)
      when is_list(upstream_node_keys) and is_map(current_upstream_states) do
    Enum.map(upstream_node_keys, fn upstream_node_key ->
      state = Map.get(current_upstream_states, upstream_node_key)

      %{
        upstream_ref: ref_from_node_key(upstream_node_key),
        upstream_node_key: upstream_node_key,
        freshness_version: freshness_version(state),
        success_run_id: success_run_id(state)
      }
    end)
  end

  @doc """
  Compares a downstream state's stored input versions to current upstream states.

  Returns `:fresh` when every planned upstream has a current state and the stored
  consumed version matches the current upstream freshness version. Otherwise it
  returns explicit stale reasons in upstream node order.
  """
  @spec freshness(
          AssetFreshnessState.t() | map(),
          Favn.Plan.plan_node() | [Favn.Plan.node_key()],
          map()
        ) ::
          :fresh | {:stale, [stale_reason()]}
  def freshness(downstream_state, %{upstream: upstream_node_keys}, current_upstream_states),
    do: freshness(downstream_state, upstream_node_keys, current_upstream_states)

  def freshness(downstream_state, upstream_node_keys, current_upstream_states)
      when is_list(upstream_node_keys) and is_map(current_upstream_states) do
    consumed_versions =
      downstream_state
      |> Map.get(:input_versions, [])
      |> normalize_input_versions()

    reasons =
      upstream_node_keys
      |> Enum.map(&stale_reason(&1, consumed_versions, current_upstream_states))
      |> Enum.reject(&is_nil/1)

    case reasons do
      [] -> :fresh
      reasons -> {:stale, reasons}
    end
  end

  @spec normalize_input_versions(map() | list()) :: %{
          optional(Favn.Plan.node_key()) => input_version()
        }
  defp normalize_input_versions(input_versions) when is_list(input_versions) do
    input_versions
    |> Enum.reduce(%{}, fn input_version, acc ->
      case normalize_input_version(input_version) do
        %{upstream_node_key: upstream_node_key} = normalized when not is_nil(upstream_node_key) ->
          Map.put(acc, upstream_node_key, normalized)

        _other ->
          acc
      end
    end)
  end

  defp normalize_input_versions(input_versions) when is_map(input_versions) do
    input_versions
    |> Enum.map(fn
      {upstream_node_key, version} when is_tuple(upstream_node_key) ->
        %{
          upstream_ref: ref_from_node_key(upstream_node_key),
          upstream_node_key: upstream_node_key,
          freshness_version: version,
          success_run_id: nil
        }

      {_key, %{} = input_version} ->
        normalize_input_version(input_version)

      {_key, _version} ->
        nil
    end)
    |> normalize_input_versions()
  end

  defp normalize_input_versions(_input_versions), do: %{}

  defp stale_reason(upstream_node_key, consumed_versions, current_upstream_states) do
    consumed = Map.get(consumed_versions, upstream_node_key)
    consumed_version = version_from_input_version(consumed)

    case Map.fetch(current_upstream_states, upstream_node_key) do
      :error ->
        reason(:missing_upstream_version, upstream_node_key, consumed_version, nil, nil)

      {:ok, current_state} ->
        current_version = freshness_version(current_state)

        if consumed_version == current_version do
          nil
        else
          reason(
            :upstream_version_changed,
            upstream_node_key,
            consumed_version,
            current_version,
            success_run_id(current_state)
          )
        end
    end
  end

  defp reason(type, upstream_node_key, consumed_version, current_version, current_success_run_id) do
    %{
      type: type,
      upstream_ref: ref_from_node_key(upstream_node_key),
      upstream_node_key: upstream_node_key,
      consumed_version: consumed_version,
      current_version: current_version,
      current_success_run_id: current_success_run_id
    }
  end

  defp normalize_input_version(%{} = input_version) do
    upstream_node_key = field(input_version, :upstream_node_key)

    %{
      upstream_ref: field(input_version, :upstream_ref) || ref_from_node_key(upstream_node_key),
      upstream_node_key: upstream_node_key,
      freshness_version: version_from_input_version(input_version),
      success_run_id:
        field(input_version, :success_run_id) || field(input_version, :current_success_run_id)
    }
  end

  defp normalize_input_version(_input_version), do: nil

  defp version_from_input_version(nil), do: nil

  defp version_from_input_version(%{} = input_version) do
    field(input_version, :freshness_version) || field(input_version, :consumed_version)
  end

  defp field(map, key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp ref_from_node_key({ref, _identity}), do: ref
  defp ref_from_node_key(_node_key), do: nil

  defp freshness_version(nil), do: nil
  defp freshness_version(%AssetFreshnessState{} = state), do: state.freshness_version
  defp freshness_version(%{} = state), do: field(state, :freshness_version)
  defp freshness_version(_state), do: nil

  defp success_run_id(nil), do: nil
  defp success_run_id(%AssetFreshnessState{} = state), do: state.latest_success_run_id
  defp success_run_id(%{} = state), do: field(state, :latest_success_run_id)
  defp success_run_id(_state), do: nil
end

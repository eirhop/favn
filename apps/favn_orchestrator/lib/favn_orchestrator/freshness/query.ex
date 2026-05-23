defmodule FavnOrchestrator.Freshness.Query do
  @moduledoc """
  Internal orchestrator queries for asset freshness and stale explanations.

  This module is intentionally below `favn_view`. It exposes control-plane query
  shapes backed by orchestrator storage without defining an external API surface.

  Callers that need the public orchestrator facade should prefer
  `FavnOrchestrator.get_asset_freshness/2`,
  `FavnOrchestrator.list_asset_freshness/1`, and
  `FavnOrchestrator.explain_asset_staleness/2`.
  """

  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Staleness
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Storage

  @type stale_explanation :: %{
          required(:asset_ref) => Favn.Ref.t(),
          required(:freshness_key) => String.t(),
          required(:status) => :fresh | :stale,
          required(:latest_success_run_id) => String.t() | nil,
          required(:stale_reasons) => [Staleness.stale_reason()]
        }

  @doc """
  Returns one stored freshness state for an asset/freshness key.

  `freshness_key` must be a stable key from `Favn.Freshness.Key`, for example
  `"latest"`, `"calendar:day:Etc/UTC:2026-05-09"`, or a window key built with
  `Favn.Freshness.Key.window!/1`.
  """
  @spec get_asset_freshness(Favn.Ref.t(), String.t()) ::
          {:ok, AssetFreshnessState.t()} | {:error, term()}
  def get_asset_freshness({module, name}, freshness_key)
      when is_atom(module) and is_atom(name) and is_binary(freshness_key) do
    Storage.get_asset_freshness_state(module, name, freshness_key)
  end

  @doc """
  Lists stored freshness states.

  Supported filters depend on the storage adapter and include the common
  orchestrator read-model filters: `:asset_ref_module`, `:asset_ref_name`,
  `:freshness_key`, `:status`, `:manifest_version_id`, plus `:limit` and
  `:offset` pagination.
  """
  @spec list_asset_freshness(keyword()) ::
          {:ok, FavnOrchestrator.Page.t(AssetFreshnessState.t())} | {:error, term()}
  def list_asset_freshness(opts \\ []) when is_list(opts),
    do: Storage.list_asset_freshness_states(opts)

  @doc """
  Explains whether a stored asset freshness state is stale against current upstream versions.

  Required options:

    * `:freshness_key` - downstream freshness key. Defaults to `"latest"`.
    * `:upstream_node_keys` - planned upstream node keys for the concrete node.

  This first query shape stays manifest-independent. Callers that need windowed or
  graph-derived explanations should pass the concrete planned upstream node keys.
  """
  @spec explain_asset_staleness(Favn.Ref.t(), keyword()) ::
          {:ok, stale_explanation()} | {:error, term()}
  def explain_asset_staleness({module, name} = ref, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    freshness_key = Keyword.get(opts, :freshness_key, Favn.Freshness.Key.latest())
    upstream_node_keys = Keyword.get(opts, :upstream_node_keys, [])

    with true <- is_binary(freshness_key) || {:error, {:invalid_freshness_key, freshness_key}},
         true <-
           is_list(upstream_node_keys) ||
             {:error, {:invalid_upstream_node_keys, upstream_node_keys}},
         {:ok, state} <- get_asset_freshness(ref, freshness_key),
         {:ok, current_upstream_states} <- current_upstream_states(upstream_node_keys) do
      {:ok, explanation(ref, state, upstream_node_keys, current_upstream_states)}
    end
  end

  defp current_upstream_states([]), do: {:ok, %{}}

  defp current_upstream_states(upstream_node_keys) do
    upstream_node_keys
    |> MapSet.new()
    |> fetch_current_upstream_states(nil, %{})
  end

  defp fetch_current_upstream_states(upstream_node_keys, cursor, acc) do
    with {:ok, page} <-
           Storage.scan_asset_freshness_states([], [{:limit, Page.max_limit()}, {:after, cursor}]) do
      acc = collect_current_upstream_states(page.items, upstream_node_keys, acc)

      cond do
        MapSet.size(upstream_node_keys) == map_size(acc) -> {:ok, acc}
        page.has_more? -> fetch_current_upstream_states(upstream_node_keys, page.next_cursor, acc)
        true -> {:ok, acc}
      end
    end
  end

  defp collect_current_upstream_states(states, upstream_node_keys, acc) do
    Enum.reduce(states, acc, fn
      %AssetFreshnessState{latest_success_node_key: node_key} = state, acc ->
        if MapSet.member?(upstream_node_keys, node_key) do
          Map.put_new(acc, node_key, state)
        else
          acc
        end

      _state, acc ->
        acc
    end)
  end

  defp explanation(
         ref,
         %AssetFreshnessState{} = state,
         upstream_node_keys,
         current_upstream_states
       ) do
    case Staleness.freshness(state, upstream_node_keys, current_upstream_states) do
      :fresh ->
        base_explanation(ref, state, :fresh, [])

      {:stale, reasons} ->
        base_explanation(ref, state, :stale, reasons)
    end
  end

  defp base_explanation(ref, %AssetFreshnessState{} = state, status, reasons) do
    %{
      asset_ref: ref,
      freshness_key: state.freshness_key,
      status: status,
      latest_success_run_id: state.latest_success_run_id,
      stale_reasons: reasons
    }
  end
end

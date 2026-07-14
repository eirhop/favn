defmodule FavnOrchestrator.Storage.Adapter.Memory.Freshness do
  @moduledoc """
  Asset freshness-state queries for the in-memory adapter.
  """

  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.Storage.Adapter.Memory.Query
  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @filters [
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :status,
    :freshness_version,
    :latest_success_run_id,
    :latest_attempt_run_id,
    :latest_attempt_status,
    :manifest_version_id,
    :manifest_content_hash
  ]

  @doc false
  @spec put(State.t(), AssetFreshnessState.t()) :: State.t()
  def put(%State{} = state, %AssetFreshnessState{} = freshness_state) do
    key = key(freshness_state)
    %{state | asset_freshness_states: Map.put(state.asset_freshness_states, key, freshness_state)}
  end

  @doc false
  def get(%State{} = state, key), do: Query.fetch(state.asset_freshness_states, key)

  @doc false
  def list(%State{} = state, filters) do
    with :ok <- Query.validate_filters(filters, @filters) do
      Query.page(Map.values(state.asset_freshness_states), filters, &sort_key/1)
    end
  end

  @doc false
  def scan(%State{} = state, filters, opts) do
    with :ok <- Query.validate_filters(filters, @filters),
         {:ok, after_key} <- cursor(Keyword.get(opts, :after)) do
      rows =
        state.asset_freshness_states
        |> Map.values()
        |> Query.filter(filters)
        |> Enum.sort_by(&sort_key/1)
        |> Query.drop_after(after_key, &sort_key/1)
        |> Enum.take(Keyword.fetch!(opts, :limit) + 1)

      {:ok, CursorPage.from_fetched(rows, opts, &cursor!/1)}
    end
  end

  @doc false
  @spec get_by_keys(State.t(), [tuple()]) :: %{optional(tuple()) => AssetFreshnessState.t()}
  def get_by_keys(%State{} = state, keys) do
    Enum.reduce(keys, %{}, fn key, acc ->
      case Map.fetch(state.asset_freshness_states, key) do
        {:ok, freshness_state} -> Map.put(acc, key, freshness_state)
        :error -> acc
      end
    end)
  end

  defp key(state), do: {state.asset_ref_module, state.asset_ref_name, state.freshness_key}

  defp cursor(nil), do: {:ok, nil}

  defp cursor(%{
         kind: :asset_freshness_state,
         updated_at: %DateTime{} = updated_at,
         asset_ref_module: module,
         asset_ref_name: name,
         freshness_key: freshness_key
       })
       when is_atom(module) and is_atom(name) and is_binary(freshness_key),
       do: {:ok, sort_key(updated_at, module, name, freshness_key)}

  defp cursor(_cursor), do: {:error, :invalid_cursor_pagination}

  defp cursor!(state) do
    %{
      kind: :asset_freshness_state,
      updated_at: state.updated_at,
      asset_ref_module: state.asset_ref_module,
      asset_ref_name: state.asset_ref_name,
      freshness_key: state.freshness_key
    }
  end

  defp sort_key(state) do
    sort_key(state.updated_at, state.asset_ref_module, state.asset_ref_name, state.freshness_key)
  end

  defp sort_key(updated_at, module, name, freshness_key) do
    {-DateTime.to_unix(updated_at, :microsecond), Atom.to_string(module), Atom.to_string(name),
     freshness_key}
  end
end

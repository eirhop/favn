defmodule FavnOrchestrator.Storage.Adapter.Memory.Logs do
  @moduledoc """
  Log persistence and query operations for the in-memory adapter.

  Producer sequence pairs are indexed for constant-time idempotency checks.
  Entries are stored newest-first so appends stay linear in the incoming batch;
  queries restore chronological order only when their contract requires it.
  """

  alias Favn.Log.Cursor
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.Storage.LogEntryCodec

  @filter_keys [
    :run_id,
    :asset_step_id,
    :runner_execution_id,
    :level,
    :source,
    :stream,
    :levels,
    :sources,
    :since,
    :until,
    :asset_ref,
    :node_key
  ]

  @doc false
  @spec persist(State.t(), [Favn.Log.Entry.t()]) :: {[Favn.Log.Entry.t()], State.t()}
  def persist(%State{} = state, entries) do
    {persisted, new_entries, index, sequence} =
      Enum.reduce(
        entries,
        {[], [], state.log_entries_by_producer_sequence, state.log_global_sequence},
        fn entry, {persisted, new_entries, index, sequence} ->
          case idempotency_key(entry) do
            {:ok, key} when is_map_key(index, key) ->
              {[Map.fetch!(index, key) | persisted], new_entries, index, sequence}

            key_result ->
              next_sequence = sequence + 1
              persisted_entry = LogEntryCodec.assign_global_sequence(entry, next_sequence)
              next_index = maybe_index(index, key_result, persisted_entry)

              {[persisted_entry | persisted], [persisted_entry | new_entries], next_index,
               next_sequence}
          end
        end
      )

    next_state = %{
      state
      | log_entries: new_entries ++ state.log_entries,
        log_entries_by_producer_sequence: index,
        log_global_sequence: sequence
    }

    {Enum.reverse(persisted), next_state}
  end

  @doc false
  @spec list(State.t(), term(), keyword()) :: {:ok, Page.t()} | {:error, term()}
  def list(%State{} = state, filter, opts) do
    with {:ok, page_opts} <- Page.normalize_opts(opts),
         {:ok, rows} <- filter(state.log_entries, filter) do
      rows =
        rows
        |> order_from_newest(Keyword.get(opts, :order, :asc))
        |> Enum.drop(Keyword.fetch!(page_opts, :offset))
        |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)

      {:ok, Page.from_fetched(rows, page_opts)}
    end
  end

  @doc false
  @spec scan(State.t(), term(), keyword()) :: {:ok, CursorPage.t()} | {:error, term()}
  def scan(%State{} = state, filter, opts) do
    with {:ok, after_sequence} <- cursor_sequence(Keyword.get(opts, :after)),
         {:ok, rows} <- filter(state.log_entries, filter) do
      rows =
        rows
        |> Enum.reverse()
        |> Enum.drop_while(&(Map.get(&1, :global_sequence, 0) <= after_sequence))
        |> Enum.take(Keyword.fetch!(opts, :limit) + 1)

      {:ok, CursorPage.from_fetched(rows, opts, &entry_cursor!/1)}
    end
  end

  @doc false
  @spec replay(State.t(), term(), term(), keyword()) ::
          {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def replay(%State{} = state, cursor, filter, opts) do
    limit = Keyword.get(opts, :limit, 200)

    with {:ok, after_sequence} <- cursor_sequence(cursor),
         :ok <- validate_replay_limit(limit),
         {:ok, rows} <- filter(state.log_entries, filter) do
      {:ok,
       rows
       |> Enum.reverse()
       |> Enum.drop_while(&(Map.get(&1, :global_sequence, 0) <= after_sequence))
       |> Enum.take(limit)}
    end
  end

  @doc false
  @spec normalize_entries([term()]) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def normalize_entries(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case LogEntryCodec.normalize(entry) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp filter(entries, filter) do
    with {:ok, filters} <- normalize_filter(filter) do
      {:ok, Enum.filter(entries, &matches?(&1, filters))}
    end
  end

  defp normalize_filter(filter) when is_list(filter) do
    if Keyword.keyword?(filter) do
      filter
      |> Keyword.drop([:limit, :offset])
      |> validate_filter()
    else
      {:error, :invalid_log_filter}
    end
  end

  defp normalize_filter(%_{} = filter), do: filter |> Map.from_struct() |> normalize_filter()

  defp normalize_filter(filter) when is_map(filter) do
    filter
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case normalize_filter_key(key) do
        {:ok, normalized_key} -> {:cont, {:ok, [{normalized_key, value} | acc]}}
        :error -> {:halt, {:error, {:unsupported_filter, key}}}
      end
    end)
    |> case do
      {:ok, filters} -> validate_filter(Enum.reverse(filters))
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_filter(_filter), do: {:error, :invalid_log_filter}

  defp validate_filter(filters) do
    Enum.reduce_while(filters, {:ok, []}, fn
      {key, value}, {:ok, acc} when key in [:levels, :sources] and is_list(value) ->
        {:cont, {:ok, [{key, value} | acc]}}

      {key, %DateTime{} = value}, {:ok, acc} when key in [:since, :until] ->
        {:cont, {:ok, [{key, value} | acc]}}

      {key, value}, {:ok, acc}
      when key in @filter_keys and key not in [:levels, :sources, :since, :until] ->
        {:cont, {:ok, [{key, value} | acc]}}

      {key, _value}, _acc ->
        {:halt, {:error, {:unsupported_filter, key}}}
    end)
    |> case do
      {:ok, validated} -> {:ok, Enum.reverse(validated)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_filter_key(key) when key in @filter_keys, do: {:ok, key}

  defp normalize_filter_key(key) when is_binary(key) do
    case Enum.find(@filter_keys, &(Atom.to_string(&1) == key)) do
      nil -> :error
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_filter_key(_key), do: :error

  defp matches?(entry, filters) do
    Enum.all?(filters, fn
      {:levels, []} -> true
      {:levels, levels} -> Enum.any?(levels, &same_atom_value?(entry.level, &1))
      {:sources, []} -> true
      {:sources, sources} -> Enum.any?(sources, &same_atom_value?(entry.source, &1))
      {:since, since} -> DateTime.compare(entry.occurred_at, since) != :lt
      {:until, until} -> DateTime.compare(entry.occurred_at, until) != :gt
      {:level, expected} -> same_atom_value?(entry.level, expected)
      {:source, expected} -> same_atom_value?(entry.source, expected)
      {:stream, expected} -> same_atom_value?(entry.stream, expected)
      {key, expected} -> Map.get(entry, key) == expected
    end)
  end

  defp same_atom_value?(actual, expected) when is_atom(actual) and is_binary(expected),
    do: Atom.to_string(actual) == expected

  defp same_atom_value?(actual, expected) when is_binary(actual) and is_atom(expected),
    do: actual == Atom.to_string(expected)

  defp same_atom_value?(actual, expected), do: actual == expected

  defp order_from_newest(entries, order) when order in [:desc, "desc"], do: entries
  defp order_from_newest(entries, _order), do: Enum.reverse(entries)

  defp idempotency_key(entry) do
    case {Map.get(entry, :producer_id), Map.get(entry, :producer_sequence)} do
      {producer_id, sequence} when is_binary(producer_id) and is_integer(sequence) ->
        {:ok, {producer_id, sequence}}

      _other ->
        :not_indexed
    end
  end

  defp maybe_index(index, {:ok, key}, entry), do: Map.put(index, key, entry)
  defp maybe_index(index, :not_indexed, _entry), do: index

  defp cursor_sequence(nil), do: {:ok, 0}
  defp cursor_sequence(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp cursor_sequence(value) when is_binary(value) do
    case Integer.parse(value) do
      {sequence, ""} when sequence >= 0 ->
        {:ok, sequence}

      _other ->
        case Cursor.parse(value) do
          {:ok, cursor} -> cursor_sequence(cursor)
          {:error, _reason} -> {:error, :cursor_invalid}
        end
    end
  end

  defp cursor_sequence(%_{} = cursor), do: cursor |> Map.from_struct() |> cursor_sequence()

  defp cursor_sequence(%{} = cursor) do
    cursor
    |> Map.get(
      :global_sequence,
      Map.get(cursor, :after_global_sequence, Map.get(cursor, "global_sequence"))
    )
    |> cursor_sequence()
  end

  defp cursor_sequence(_cursor), do: {:error, :cursor_invalid}

  defp entry_cursor!(%Favn.Log.Entry{} = entry) do
    %{kind: :log_entry, global_sequence: entry.global_sequence}
  end

  defp validate_replay_limit(limit) when is_integer(limit) and limit > 0, do: :ok
  defp validate_replay_limit(_limit), do: {:error, :cursor_invalid}
end
